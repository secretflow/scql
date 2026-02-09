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

#include "engine/util/datamesh_helper.h"

#include "yacl/base/exception.h"

namespace scql::engine::util {

dm::DomainData QueryDomainData(std::shared_ptr<grpc::Channel> channel,
                               const std::string& domain_data_id) {
  dm::DomainDataService::Stub stub(channel);
  grpc::ClientContext context;

  dm::QueryDomainDataRequest request;
  request.set_domaindata_id(domain_data_id);

  dm::QueryDomainDataResponse resp;
  auto status = stub.QueryDomainData(&context, request, &resp);
  if (!status.ok()) {
    YACL_THROW("issue grpc QueryDomainData failed, error_code={}, error_msg={}",
               fmt::underlying(status.error_code()), status.error_message());
  }

  if (resp.status().code() != 0) {
    YACL_THROW("QueryDomainData returns error: code={}, msg={}",
               resp.status().code(), resp.status().message());
  }

  return resp.data();
}

dm::DomainDataSource QueryDomainDataSource(
    std::shared_ptr<grpc::Channel> channel, const std::string& datasource_id) {
  dm::DomainDataSourceService::Stub stub(channel);

  grpc::ClientContext context;

  dm::QueryDomainDataSourceRequest request;
  request.set_datasource_id(datasource_id);

  dm::QueryDomainDataSourceResponse resp;
  auto status = stub.QueryDomainDataSource(&context, request, &resp);
  if (!status.ok()) {
    YACL_THROW(
        "issue grpc QueryDomainDataSource failed, error_code={}, error_msg={}",
        fmt::underlying(status.error_code()), status.error_message());
  }
  if (resp.status().code() != 0) {
    YACL_THROW("QueryDomainDataSource returns error: code={}, msg={}",
               resp.status().code(), resp.status().message());
  }

  return resp.data();
}

}  // namespace scql::engine::util