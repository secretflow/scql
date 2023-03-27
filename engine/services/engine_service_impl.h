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

#include "engine/datasource/datasource_adaptor_mgr.h"
#include "engine/datasource/embed_router.h"
#include "engine/framework/session_manager.h"
#include "engine/link/channel_manager.h"

#include "api/engine.pb.h"

namespace scql::engine {

struct EngineServiceOptions {
  bool enable_authorization = false;
  std::string credential;
};

class EngineServiceImpl : public pb::SCQLEngineService {
 public:
  EngineServiceImpl(const EngineServiceOptions& options,
                    std::unique_ptr<SessionManager> session_mgr,
                    ChannelManager* channel_manager);

  ~EngineServiceImpl() = default;

  void StartSession(::google::protobuf::RpcController* cntl,
                    const pb::StartSessionRequest* request,
                    pb::StartSessionResponse* response,
                    ::google::protobuf::Closure* done) override;

  // Async api, just enqueue RunDag request and return, callback after
  // finished.
  void RunDag(::google::protobuf::RpcController* cntl,
              const pb::RunDagRequest* request, pb::RunDagResponse* response,
              ::google::protobuf::Closure* done) override;

  void StopSession(::google::protobuf::RpcController* cntl,
                   const pb::StopSessionRequest* request,
                   pb::StopSessionResponse* response,
                   ::google::protobuf::Closure* done) override;

  void RunExecutionPlan(::google::protobuf::RpcController* cntl,
                        const pb::RunExecutionPlanRequest* request,
                        pb::RunExecutionPlanResponse* response,
                        ::google::protobuf::Closure* done) override;

 private:
  void RunDagWithSession(const pb::RunDagRequest request, Session* session);

  std::string ConstructReportInfo(const pb::Status& status,
                                  const pb::RunDagRequest& request,
                                  Session* session);

  void ReportToScdb(const pb::RunDagRequest& request,
                    const std::string& report_info_str);

  void RunPlan(const pb::RunExecutionPlanRequest& request, Session* session,
               pb::RunExecutionPlanResponse* response);

  bool CheckSCDBCredential(const brpc::HttpHeader& http_header);

 private:
  const EngineServiceOptions service_options_;
  std::unique_ptr<SessionManager> session_mgr_;
  // thread pool to run RunDag tasks.
  yacl::ThreadPool worker_pool_;

  ChannelManager* channel_manager_;

  // used for datasource operator.
  std::unique_ptr<Router> ds_router_;
  std::unique_ptr<DatasourceAdaptorMgr> ds_mgr_;
};

}  // namespace scql::engine
