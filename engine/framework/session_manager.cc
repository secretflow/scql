// Copyright 2023 Ant Group Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "engine/framework/session_manager.h"

#include <memory>

namespace scql::engine {

SessionManager::SessionManager(
    const SessionOptions& session_opt, ListenerManager* listener_manager,
    std::unique_ptr<yacl::link::ILinkFactory> link_factory,
    std::unique_ptr<Router> ds_router,
    std::unique_ptr<DatasourceAdaptorMgr> ds_mgr, int32_t session_timeout_s)
    : session_opt_(session_opt),
      listener_manager_(listener_manager),
      link_factory_(std::move(link_factory)),
      ds_router_(std::move(ds_router)),
      ds_mgr_(std::move(ds_mgr)),
      session_default_timeout_s_(session_timeout_s) {
  watch_thread_.reset(
      new std::thread([this]() { this->WatchSessionTimeoutThread(); }));
}

SessionManager::~SessionManager() {
  if (watch_thread_) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      to_stop_ = true;
    }
    cv_stop_.notify_one();
    SPDLOG_INFO("session manager notify watch thread to stop");
    try {
      watch_thread_->join();
      SPDLOG_INFO("session manager joined the watch thread");
    } catch (std::exception& ex) {
      SPDLOG_ERROR("session manager wait watch thread failed, err={}",
                   ex.what());
    }
  }
}

void SessionManager::CreateSession(const pb::SessionStartParams& params) {
  const std::string& session_id = params.session_id();
  YACL_ENFORCE(!session_id.empty(), "session_id is empty.");

  // setup logger for new session
  // sinks should be the same as the default logger
  // If different loggers aim to write to the same output file all of them
  // must share the same sink. Otherwise there will be strange results.
  const auto& sinks = spdlog::default_logger()->sinks();
  std::string logger_name = "session(" + session_id + ")";
  auto session_logger =
      std::make_shared<spdlog::logger>(logger_name, sinks.begin(), sinks.end());

  auto new_session = std::make_unique<Session>(
      session_opt_, params, link_factory_.get(), session_logger,
      ds_router_.get(), ds_mgr_.get());
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (id_to_session_.find(session_id) != id_to_session_.end()) {
      YACL_THROW_LOGIC_ERROR("Session for session_id={} exists.", session_id);
    }

    auto end = std::chrono::system_clock::now();
    auto time_cost = std::chrono::duration_cast<std::chrono::milliseconds>(
                         end - new_session->GetStartTime())
                         .count();

    id_to_session_.emplace(session_id, std::move(new_session));

    session_timeout_queue_.push(session_id);

    SPDLOG_INFO(
        "create session({}) succ, cost({}ms), current running session={}",
        session_id, time_cost, id_to_session_.size());
  }
}

Session* SessionManager::GetSession(const std::string& session_id) {
  if (session_id.empty()) {
    SPDLOG_WARN("session_id is empty, default return nullptr.");
    return nullptr;
  }
  std::unique_lock<std::mutex> lock(mutex_);
  auto iter = id_to_session_.find(session_id);
  if (iter == id_to_session_.end()) {
    SPDLOG_WARN("session({}) not exists. default return nullptr.", session_id);
    return nullptr;
  }

  return iter->second.get();
}

void SessionManager::RemoveSession(const std::string& session_id) {
  if (session_id.empty()) {
    SPDLOG_WARN("session_id is empty.");
    return;
  }

  {
    std::unique_lock<std::mutex> lock(mutex_);

    auto iter = id_to_session_.find(session_id);
    if (iter == id_to_session_.end()) {
      SPDLOG_WARN("session({}) not exists.", session_id);
      return;
    }

    if (iter->second->GetState() != SessionState::IDLE) {
      YACL_THROW_LOGIC_ERROR("session({}), status({}) != idle, so can't stop.",
                             session_id,
                             static_cast<size_t>(iter->second->GetState()));
    }

    auto node_handle = id_to_session_.extract(iter);
    // unlock in time, since the following WaitLinkTaskFinish() may take a while
    // to run
    lock.unlock();

    auto lctx = node_handle.mapped()->GetLink();
    if (lctx) {
      lctx->WaitLinkTaskFinish();
    }
    auto end = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end - node_handle.mapped()->GetStartTime());
    SPDLOG_INFO(
        "session({}) removed, running_cost({}ms), current running session={}",
        session_id, duration.count(), id_to_session_.size());
  }

  listener_manager_->RemoveListener(session_id);
}

bool SessionManager::SetSessionState(const std::string& session_id,
                                     SessionState expect_current_state,
                                     SessionState state) {
  std::unique_lock<std::mutex> lock(mutex_);
  auto iter = id_to_session_.find(session_id);
  if (iter == id_to_session_.end()) {
    SPDLOG_WARN("session({}) not exists.", session_id);
    return false;
  }

  if (iter->second->GetState() != expect_current_state) {
    SPDLOG_WARN(
        "session({}), set session status failed. current status({}) != {}",
        session_id, static_cast<size_t>(iter->second->GetState()),
        static_cast<size_t>(expect_current_state));
    return false;
  }

  iter->second->SetState(state);
  SPDLOG_INFO("session({}), set old state={} to new state={}", session_id,
              static_cast<size_t>(expect_current_state),
              static_cast<size_t>(state));
  return true;
}

void SessionManager::WatchSessionTimeoutThread() {
  SPDLOG_INFO("WatchSessionTimeoutThread startup, session default timeout={}s",
              session_default_timeout_s_.count());
  while (!to_stop_) {
    // 1.get next check time.
    auto next_check_time = NextCheckTime();

    // 2.sleep until next check time and break if to_stop_ marked.
    {
      std::unique_lock<std::mutex> lock(mutex_);

      while (!to_stop_ && std::chrono::system_clock::now() < next_check_time) {
        cv_stop_.wait_until(lock, next_check_time);
      }
    }

    if (to_stop_) {
      break;
    }

    // 3.find one timeout session.
    std::optional<std::string> timeout_session = GetTimeoutSession();

    // 4.remove session.
    if (timeout_session.has_value()) {
      try {
        RemoveSession(timeout_session.value());
        SPDLOG_WARN("[TIMEOUT] session({}) removed due to timeout",
                    timeout_session.value());
      } catch (std::exception& ex) {
        SPDLOG_WARN(
            "[TIMEOUT] remove session({}) failed, err={}, sleep 60s before "
            "retry",
            timeout_session.value(), ex.what());
        sleep(60);
      }
    }
  }
  SPDLOG_INFO("Watch thread stopped.");
}

std::chrono::time_point<std::chrono::system_clock>
SessionManager::NextCheckTime() {
  // set default-check-interval to 60s empirically to avoid growth of
  // session_timeout_queue_.
  auto default_check_interval =
      std::min(session_default_timeout_s_, std::chrono::seconds(60));
  auto result = std::chrono::system_clock::now() + default_check_interval;
  std::unique_lock<std::mutex> lock(mutex_);
  while (!session_timeout_queue_.empty()) {
    const auto& session = session_timeout_queue_.front();
    auto iter = id_to_session_.find(session);
    if (iter == id_to_session_.end()) {
      session_timeout_queue_.pop();
    } else {
      return std::min(
          result, iter->second->GetStartTime() + session_default_timeout_s_);
    }
  }

  return result;
}

std::optional<std::string> SessionManager::GetTimeoutSession() {
  auto now = std::chrono::system_clock::now();
  std::unique_lock<std::mutex> lock(mutex_);
  while (!session_timeout_queue_.empty()) {
    const auto& session = session_timeout_queue_.front();
    auto iter = id_to_session_.find(session);
    if (iter == id_to_session_.end()) {
      session_timeout_queue_.pop();
      continue;
    }

    if (iter->second->GetStartTime() + session_default_timeout_s_ <= now) {
      return session;
    } else {
      return std::nullopt;
    }
  }

  return std::nullopt;
}

}  // namespace scql::engine