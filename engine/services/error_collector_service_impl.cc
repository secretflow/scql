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

#include "engine/services/error_collector_service_impl.h"

#include "brpc/closure_guard.h"

#include "api/status_code.pb.h"

namespace scql::engine {

void ErrorCollectorServiceImpl::ReportError(
    ::google::protobuf::RpcController* cntl,
    const services::pb::ReportErrorRequest* request,
    services::pb::ReportErrorResponse* response,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  try {
    const std::string& job_id = request->job_id();
    Session* session = session_manager_->GetSession(job_id);
    if (session == nullptr) {
      response->mutable_status()->set_code(pb::Code::SESSION_NOT_FOUND);
      response->mutable_status()->set_message(
          fmt::format("no session for job_id={}", job_id));
      return;
    }

    session->StorePeerError(request->party_code(), request->status());

    response->mutable_status()->set_code(pb::Code::OK);
    return;
  } catch (const std::exception& e) {
    response->mutable_status()->set_code(pb::Code::UNKNOWN_ENGINE_ERROR);
    response->mutable_status()->set_message(fmt::format(
        "internal error, job_id={}, error={}", request->job_id(), e.what()));
    return;
  }
}

}  // namespace scql::engine
