// Copyright 2025 Ant Group Co., Ltd.
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

#include "grpcpp/grpcpp.h"

#include "kuscia/proto/api/v1alpha1/datamesh/domaindata.grpc.pb.h"
#include "kuscia/proto/api/v1alpha1/datamesh/domaindatasource.grpc.pb.h"

namespace scql::engine::util {

namespace dm = kuscia::proto::api::v1alpha1::datamesh;

dm::DomainData QueryDomainData(std::shared_ptr<grpc::Channel> channel,
                               const std::string& domain_data_id);

dm::DomainDataSource QueryDomainDataSource(
    std::shared_ptr<grpc::Channel> channel, const std::string& datasource_id);

}  // namespace scql::engine::util