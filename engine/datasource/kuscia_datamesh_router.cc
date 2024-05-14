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

#include "engine/datasource/kuscia_datamesh_router.h"

#include "absl/strings/str_split.h"
#include "butil/files/file_path.h"
#include "google/protobuf/util/json_util.h"
#include "grpcpp/grpcpp.h"
#include "yacl/base/exception.h"

#include "engine/datasource/csvdb_conf.pb.h"
#include "kuscia/proto/api/v1alpha1/datamesh/domaindata.grpc.pb.h"
#include "kuscia/proto/api/v1alpha1/datamesh/domaindatasource.grpc.pb.h"

namespace scql::engine {

namespace dm = kuscia::proto::api::v1alpha1::datamesh;

namespace {

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

DataSource MakeCSVDataSourceFromLocalfs(
    const std::string& datasource_id, const std::string& name,
    const dm::DomainData& domaindata, const dm::LocalDataSourceInfo& localfs) {
  DataSource result;
  result.set_id(domaindata.domaindata_id());
  result.set_name(domaindata.name());
  result.set_kind(DataSourceKind::CSVDB);
  // constuct connection str
  {
    csv::CsvdbConf csv_conf;
    auto csv_tbl = csv_conf.add_tables();

    csv_tbl->set_table_name(domaindata.domaindata_id());
    butil::FilePath path(localfs.path());
    path = path.Append(domaindata.relative_uri());
    csv_tbl->set_data_path(path.value());
    for (const auto& column : domaindata.columns()) {
      auto new_col = csv_tbl->add_columns();
      new_col->set_column_name(column.name());
      new_col->set_column_type(column.type());
    }

    std::string connection_str;
    auto status =
        google::protobuf::util::MessageToJsonString(csv_conf, &connection_str);
    YACL_ENFORCE(status.ok(), "failed to convert CsvdbConf to json string: {}",
                 status.ToString());
    result.set_connection_str(connection_str);
  }

  return result;
}

DataSource MakeCSVDataSourceFromOSS(const std::string& datasource_id,
                                    const std::string& name,
                                    const dm::DomainData& domaindata,
                                    const dm::OssDataSourceInfo& oss) {
  DataSource result;
  result.set_id(domaindata.domaindata_id());
  result.set_name(domaindata.name());
  result.set_kind(DataSourceKind::CSVDB);
  // constuct connection str
  {
    csv::CsvdbConf csv_conf;

    auto s3_conf = csv_conf.mutable_s3_conf();
    s3_conf->set_endpoint(oss.endpoint());
    s3_conf->set_access_key_id(oss.access_key_id());
    s3_conf->set_secret_access_key(oss.access_key_secret());

    auto csv_tbl = csv_conf.add_tables();
    csv_tbl->set_table_name(domaindata.domaindata_id());
    std::string s3_url = oss.storage_type() + "://" + oss.bucket();
    if (!oss.prefix().empty()) {
      s3_url += "/" + oss.prefix();
    }
    s3_url += "/" + domaindata.relative_uri();
    csv_tbl->set_data_path(s3_url);
    for (const auto& column : domaindata.columns()) {
      auto new_col = csv_tbl->add_columns();
      new_col->set_column_name(column.name());
      new_col->set_column_type(column.type());
    }

    std::string connection_str;
    auto status =
        google::protobuf::util::MessageToJsonString(csv_conf, &connection_str);
    YACL_ENFORCE(status.ok(), "failed to convert CsvdbConf to json string: {}",
                 status.ToString());
    result.set_connection_str(connection_str);
  }

  return result;
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

}  // namespace

KusciaDataMeshRouter::KusciaDataMeshRouter(
    const std::string& endpoint,
    const std::shared_ptr<grpc::ChannelCredentials>& credentials)
    : endpoint_(endpoint), creds_(credentials) {}

std::vector<DataSource> KusciaDataMeshRouter::Route(
    const std::vector<std::string>& table_refs) {
  std::vector<DataSource> result;
  // TODO: set timeout and retry policy
  auto channel = grpc::CreateChannel(endpoint_, creds_);
  for (auto table_ref : table_refs) {
    result.push_back(SingleRoute(channel, table_ref));
  }
  return result;
}

DataSource KusciaDataMeshRouter::SingleRoute(
    std::shared_ptr<grpc::Channel> channel, const std::string& domaindata_id) {
  auto domaindata = QueryDomainData(channel, domaindata_id);
  if (domaindata.datasource_id().empty()) {
    YACL_THROW("datasource_id is empty for domaindata_id={}", domaindata_id);
  }

  // TODO(optimization): use cache to speed-up
  auto datasource = QueryDomainDataSource(channel, domaindata.datasource_id());

  if (datasource.type() == "localfs") {
    return MakeCSVDataSourceFromLocalfs(datasource.datasource_id(),
                                        datasource.name(), domaindata,
                                        datasource.info().localfs());
  } else if (datasource.type() == "oss") {
    return MakeCSVDataSourceFromOSS(datasource.datasource_id(),
                                    datasource.name(), domaindata,
                                    datasource.info().oss());
  } else {
    YACL_THROW("unsupported datasource type: {}", datasource.type());
  }
}

}  // namespace scql::engine