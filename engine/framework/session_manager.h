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

#pragma once

#include <chrono>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>

#include "engine/datasource/datasource_adaptor_mgr.h"
#include "engine/datasource/router.h"
#include "engine/framework/session.h"
#include "engine/link/listener.h"

namespace scql::engine {

class SessionManager {
 public:
  SessionManager(const SessionOptions& session_opt,
                 ListenerManager* listener_manager,
                 std::unique_ptr<yacl::link::ILinkFactory> link_factory,
                 std::unique_ptr<Router> ds_router,
                 std::unique_ptr<DatasourceAdaptorMgr> ds_mgr,
                 int32_t session_timeout_s,
                 const std::vector<spu::ProtocolKind>& allowed_spu_protocols);

  ~SessionManager();

  SessionManager(const SessionManager&) = delete;
  SessionManager& operator=(const SessionManager&) = delete;

  void CreateSession(const pb::JobStartParams& params,
                     pb::DebugOptions debug_opts);

  Session* GetSession(const std::string& session_id);

  void StopSession(const std::string& session_id);

  void RemoveSession(const std::string& session_id);

  // Set Session state
  // return false if session not found
  bool SetSessionState(const std::string& session_id, SessionState dest_state);

 private:
  void WatchSessionTimeoutThread();

  std::chrono::time_point<std::chrono::system_clock> NextCheckTime();

  std::optional<std::string> GetTimeoutSession();

  scql::engine::SessionOptions GenerateUpdatedSessionOptions(
      const pb::JobStartParams& jobParams);

 private:
  // used to construct session
  const SessionOptions session_opt_;
  ListenerManager* listener_manager_;
  std::unique_ptr<yacl::link::ILinkFactory> link_factory_;
  std::unique_ptr<Router> ds_router_;
  std::unique_ptr<DatasourceAdaptorMgr> ds_mgr_;
  mutable std::mutex mutex_;
  std::map<std::string, std::unique_ptr<Session>> id_to_session_;

  // variables for session TTL management.
  std::chrono::seconds session_default_timeout_s_;
  std::condition_variable cv_stop_;
  std::atomic<bool> to_stop_{false};
  std::unique_ptr<std::thread> watch_thread_;
  std::queue<std::string> session_timeout_queue_;
  const std::vector<spu::ProtocolKind> allowed_spu_protocols_;
};

}  // namespace scql::engine