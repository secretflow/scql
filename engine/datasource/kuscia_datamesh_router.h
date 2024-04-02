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

#include "grpcpp/channel.h"
#include "grpcpp/security/credentials.h"

#include "engine/datasource/router.h"

namespace scql::engine {

class KusciaDataMeshRouter final : public Router {
 public:
  KusciaDataMeshRouter(
      const std::string& endpoint,
      const std::shared_ptr<grpc::ChannelCredentials>& credentials);

  std::vector<DataSource> Route(
      const std::vector<std::string>& table_refs) override;

 private:
  DataSource SingleRoute(std::shared_ptr<grpc::Channel> channel,
                         const std::string& domaindata_id);

 private:
  const std::string endpoint_;
  std::shared_ptr<grpc::ChannelCredentials> creds_;
};

};  // namespace scql::engine