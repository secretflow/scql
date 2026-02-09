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

#include "engine/util/upload_info_helper.h"

#include <map>
#include <string>
#include <vector>

#include "arrow/api.h"

#include "kuscia/proto/api/v1alpha1/common.pb.h"

namespace scql::engine::util {
std::optional<dataproxy_sdk::pb::UploadInfo> ParseUploadInfoFromString(
    const std::string& serialized_upload_info_str) {
  dataproxy_sdk::pb::UploadInfo upload_info;
  if (upload_info.ParseFromString(serialized_upload_info_str)) {
    return upload_info;
  } else {
    return std::nullopt;
  }
}

dataproxy_sdk::proto::UploadInfo ConvertProtoToClass(
    const dataproxy_sdk::pb::UploadInfo& source_upload_info) {
  dataproxy_sdk::proto::UploadInfo target_upload_info;

  target_upload_info.set_domaindata_id(source_upload_info.domaindata_id());
  target_upload_info.set_name(source_upload_info.name());
  target_upload_info.set_type(source_upload_info.type());
  target_upload_info.set_relative_uri(source_upload_info.relative_uri());
  target_upload_info.set_datasource_id(source_upload_info.datasource_id());
  target_upload_info.set_vendor(source_upload_info.vendor());

  std::map<std::string, std::string> temp_attributes;
  for (const auto& pair : source_upload_info.attributes()) {
    temp_attributes[pair.first] = pair.second;
  }
  target_upload_info.set_attributes(temp_attributes);

  std::vector<dataproxy_sdk::proto::DataColumn> temp_columns;

  for (const kuscia::proto::api::v1alpha1::DataColumn& proto_column :
       source_upload_info.columns()) {
    dataproxy_sdk::proto::DataColumn custom_column;
    custom_column.set_name(proto_column.name());
    custom_column.set_type(proto_column.type());
    custom_column.set_comment(proto_column.comment());
    custom_column.set_not_nullable(proto_column.not_nullable());
    temp_columns.push_back(custom_column);
  }

  target_upload_info.set_columns(temp_columns);

  return target_upload_info;
}

// This function discards all existing columns in the UploadInfo object and
// generates a completely new list of columns from the provided schema.
void ResetColumnsFromSchema(const std::shared_ptr<arrow::Schema>& schema,
                            dataproxy_sdk::proto::UploadInfo& upload_info) {
  std::vector<dataproxy_sdk::proto::DataColumn> synced_columns;

  for (const auto& field : schema->fields()) {
    std::string name = field->name();
    std::string type = ArrowToKusciaType(field->type());
    bool not_nullable = !field->nullable();

    dataproxy_sdk::proto::DataColumn new_column;
    new_column.set_name(name);
    new_column.set_type(type);
    new_column.set_not_nullable(not_nullable);

    synced_columns.push_back(new_column);
  }

  upload_info.set_columns(synced_columns);
}

// TODO: using arrow schema instaed of kuscia domaindata
// kuscia supported types:
// https://github.com/secretflow/dataproxy/blob/main/dataproxy-common/src/main/java/org/secretflow/dataproxy/common/utils/ArrowUtil.java#L32
// arrow types:
// https://github.com/apache/arrow/blob/main/cpp/src/arrow/type_fwd.h#L322
std::string ArrowToKusciaType(const std::shared_ptr<arrow::DataType>& dtype) {
  switch (dtype->id()) {
    case arrow::Type::FLOAT:
      return "float32";
    case arrow::Type::DOUBLE:
      return "float64";
    case arrow::Type::LARGE_STRING:
      return "string";
      // TODO: support timestamp[s]
    default:
      return dtype->ToString();
  }
  return "";
}

}  // namespace scql::engine::util