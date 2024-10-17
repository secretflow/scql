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

#include "engine/util/logging.h"

DEFINE_int32(psi_curve_type, psi::CURVE_FOURQ, "curve type used in PSI");
DEFINE_int64(unbalance_psi_ratio_threshold, 5,
             "minimum LargePartySize/SmallPartySize ratio to choose unbalanced "
             "PSI, LargePartySize means the rows count of the larger party");
DEFINE_int64(unbalance_psi_larger_party_rows_count_threshold, 81920,
             "minimum rows count of the larger party to choose unbalanced PSI");
namespace scql::engine {

SessionManager::SessionManager(
    const SessionOptions& session_opt, ListenerManager* listener_manager,
    std::unique_ptr<yacl::link::ILinkFactory> link_factory,
    std::unique_ptr<Router> ds_router,
    std::unique_ptr<DatasourceAdaptorMgr> ds_mgr, int32_t session_timeout_s,
    const std::vector<spu::ProtocolKind>& allowed_spu_protocols)
    : session_opt_(session_opt),
      listener_manager_(listener_manager),
      link_factory_(std::move(link_factory)),
      ds_router_(std::move(ds_router)),
      ds_mgr_(std::move(ds_mgr)),
      session_default_timeout_s_(session_timeout_s),
      allowed_spu_protocols_(allowed_spu_protocols) {
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

scql::engine::SessionOptions SessionManager::GenerateUpdatedSessionOptions(
    const pb::JobStartParams& params) {
  struct SessionOptions session_opt = session_opt_;
  // to make the config unit consistent, here to use second instead of
  // miliseconds
  if (params.link_cfg().link_recv_timeout_sec() > 0) {
    session_opt.link_config.link_recv_timeout_ms =
        params.link_cfg().link_recv_timeout_sec() * 1000;
  }

  if (params.link_cfg().link_throttle_window_size() > 0) {
    session_opt.link_config.link_throttle_window_size =
        params.link_cfg().link_throttle_window_size();
  }

  if (params.link_cfg().link_chunked_send_parallel_size() > 0) {
    session_opt.link_config.link_chunked_send_parallel_size =
        params.link_cfg().link_chunked_send_parallel_size();
  }

  if (params.link_cfg().http_max_payload_size() > 0) {
    session_opt.link_config.http_max_payload_size =
        params.link_cfg().http_max_payload_size();
  }

  if (params.psi_cfg().unbalance_psi_larger_party_rows_count_threshold() > 0) {
    session_opt.psi_config.unbalance_psi_larger_party_rows_count_threshold =
        params.psi_cfg().unbalance_psi_larger_party_rows_count_threshold();
  } else {
    session_opt.psi_config.unbalance_psi_larger_party_rows_count_threshold =
        FLAGS_unbalance_psi_larger_party_rows_count_threshold;
  }

  if (params.psi_cfg().unbalance_psi_ratio_threshold() > 0) {
    session_opt.psi_config.unbalance_psi_ratio_threshold =
        params.psi_cfg().unbalance_psi_ratio_threshold();
  } else {
    session_opt.psi_config.unbalance_psi_ratio_threshold =
        FLAGS_unbalance_psi_ratio_threshold;
  }

  if (params.psi_cfg().psi_curve_type() > 0) {
    session_opt.psi_config.psi_curve_type = params.psi_cfg().psi_curve_type();
  } else {
    session_opt.psi_config.psi_curve_type = FLAGS_psi_curve_type;
  }

  session_opt.log_config.enable_session_logger_separation =
      params.log_cfg().enable_session_logger_separation();

  return session_opt;
}

void SessionManager::CreateSession(const pb::JobStartParams& params,
                                   pb::DebugOptions debug_opts) {
  const std::string& job_id = params.job_id();
  YACL_ENFORCE(!job_id.empty(), "job_id is empty.");

  auto session_opt = GenerateUpdatedSessionOptions(params);

  auto new_session = std::make_unique<Session>(
      session_opt, params, debug_opts, link_factory_.get(), ds_router_.get(),
      ds_mgr_.get(), allowed_spu_protocols_);
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (id_to_session_.find(job_id) != id_to_session_.end()) {
      YACL_THROW_LOGIC_ERROR("Session for job_id={} exists.", job_id);
    }

    auto end = std::chrono::system_clock::now();
    auto time_cost = std::chrono::duration_cast<std::chrono::milliseconds>(
                         end - new_session->GetStartTime())
                         .count();

    id_to_session_.emplace(job_id, std::move(new_session));

    session_timeout_queue_.push(job_id);

    SPDLOG_INFO(
        "create session({}) succ, cost({}ms), current running session={}",
        job_id, time_cost, id_to_session_.size());
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

void SessionManager::StopSession(const std::string& session_id) {
  if (session_id.empty()) {
    SPDLOG_WARN("session_id is empty.");
    return;
  }

  std::unique_lock<std::mutex> lock(mutex_);
  auto iter = id_to_session_.find(session_id);
  lock.unlock();

  if (iter == id_to_session_.end()) {
    SPDLOG_WARN("session({}) not exists.", session_id);
    return;
  }

  if (iter->second->GetState() == SessionState::RUNNING) {
    // CAS to avoid RunPlanSync setting state concurrently
    if (iter->second->CASState(SessionState::RUNNING, SessionState::ABORTING)) {
      iter->second->GetLink()->AbortLink();
    }
  }

  SPDLOG_INFO("session({}) is stopping, waiting for task finish...",
              session_id);
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

    auto session_state = iter->second->GetState();

    if (session_state != SessionState::FAILED &&
        session_state != SessionState::SUCCEEDED) {
      YACL_THROW_LOGIC_ERROR(
          "session({}), status({}) not belong to FAILED/SUCCEEDED, so can't "
          "stop.",
          session_id, static_cast<size_t>(session_state));
    }

    auto node_handle = id_to_session_.extract(iter);
    // unlock in time, since the following WaitLinkTaskFinish() may take a while
    // to run
    lock.unlock();

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
                                     SessionState state) {
  std::unique_lock<std::mutex> lock(mutex_);
  auto iter = id_to_session_.find(session_id);
  if (iter == id_to_session_.end()) {
    SPDLOG_WARN("session({}) not exists.", session_id);
    return false;
  }

  iter->second->SetState(state);
  SPDLOG_INFO("session({}), set state={}", session_id,
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