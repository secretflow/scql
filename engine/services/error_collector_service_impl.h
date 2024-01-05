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

#include "engine/framework/session_manager.h"

#include "engine/services/error_collector_service.pb.h"

namespace scql::engine {

class ErrorCollectorServiceImpl : public services::pb::ErrorCollectorService {
 public:
  explicit ErrorCollectorServiceImpl(SessionManager* session_manager)
      : session_manager_(session_manager) {}

  void ReportError(::google::protobuf::RpcController* cntl,
                   const services::pb::ReportErrorRequest* request,
                   services::pb::ReportErrorResponse* response,
                   ::google::protobuf::Closure* done) override;

 private:
  SessionManager* session_manager_;
};

}  // namespace scql::engine