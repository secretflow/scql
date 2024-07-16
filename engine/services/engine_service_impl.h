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

#include "yacl/utils/thread_pool.h"

#include "engine/auth/authenticator.h"
#include "engine/datasource/datasource_adaptor_mgr.h"
#include "engine/datasource/embed_router.h"
#include "engine/framework/session_manager.h"
#include "engine/link/channel_manager.h"

#include "api/common.pb.h"
#include "api/engine.pb.h"
#include "api/status.pb.h"
#include "api/status_code.pb.h"
#include "engine/audit/audit.pb.h"

namespace scql::engine {

struct EngineServiceOptions {
  bool enable_authorization = false;
  std::string credential;
};

class EngineServiceImpl : public pb::SCQLEngineService {
 public:
  EngineServiceImpl(const EngineServiceOptions& options,
                    std::unique_ptr<SessionManager> session_mgr,
                    ChannelManager* channel_manager,
                    std::unique_ptr<auth::Authenticator> authenticator);

  ~EngineServiceImpl() = default;

  void RunExecutionPlan(::google::protobuf::RpcController* cntl,
                        const pb::RunExecutionPlanRequest* request,
                        pb::RunExecutionPlanResponse* response,
                        ::google::protobuf::Closure* done) override;

  void QueryJobStatus(::google::protobuf::RpcController* cntl,
                      const pb::QueryJobStatusRequest* request,
                      pb::QueryJobStatusResponse* response,
                      ::google::protobuf::Closure* done) override;

  void StopJob(::google::protobuf::RpcController* cntl,
               const pb::StopJobRequest* request, pb::Status* response,
               ::google::protobuf::Closure* done) override;

  SessionManager* GetSessionManager() { return session_mgr_.get(); }

 private:
  void ReportResult(const std::string& session_id, const std::string& cb_url,
                    const std::string& report_info_str);

  void RunPlanCore(const pb::RunExecutionPlanRequest& request, Session* session,
                   pb::RunExecutionPlanResponse* response);

  void RunPlanSync(const pb::RunExecutionPlanRequest* request, Session* session,
                   pb::RunExecutionPlanResponse* response);

  void RunPlanAsync(const pb::RunExecutionPlanRequest request, Session* session,
                    const std::string& source_ip);

  void CheckDriverCredential(const brpc::HttpHeader& http_header);

  void VerifyPublicKeys(const pb::JobStartParams& start_params);

  void ReportErrorToPeers(const pb::JobStartParams& params,
                          const pb::Code err_code, const std::string& err_msg);

  std::shared_ptr<spdlog::logger> GetActiveLogger(
      const std::string& session_id) const;

 private:
  const EngineServiceOptions service_options_;
  std::unique_ptr<SessionManager> session_mgr_;
  // thread pool to run async tasks.
  yacl::ThreadPool worker_pool_;

  ChannelManager* channel_manager_;

  // used for datasource operator.
  std::unique_ptr<Router> ds_router_;
  std::unique_ptr<DatasourceAdaptorMgr> ds_mgr_;

  std::unique_ptr<auth::Authenticator> authenticator_;
};

}  // namespace scql::engine