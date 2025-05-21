// Copyright 2024 Ant Group Co., Ltd.
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

#include "engine/datasource/dp_adaptor.h"

#include <cstddef>
#include <filesystem>
#include <future>
#include <memory>
#include <optional>
#include <vector>

#include "absl/strings/ascii.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/compute/cast.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "dataproxy_sdk/data_proxy_stream.h"
#include "gflags/gflags.h"

#include "engine/exe/flags.h"

namespace scql::engine {

std::shared_ptr<ChunkedResult> DpAdaptor::SendQuery(const std::string& query) {
  dataproxy_sdk::proto::DataProxyConfig config;
  config.set_data_proxy_addr(FLAGS_kuscia_datamesh_endpoint);
  config.mutable_tls_config()->set_certificate_path(
      FLAGS_kuscia_datamesh_client_cert_path);
  config.mutable_tls_config()->set_private_key_path(
      FLAGS_kuscia_datamesh_client_key_path);
  config.mutable_tls_config()->set_ca_file_path(
      FLAGS_kuscia_datamesh_cacert_path);
  auto stream = dataproxy_sdk::DataProxyStream::Make(config);
  YACL_ENFORCE(stream != nullptr);
  dataproxy_sdk::proto::SQLInfo sql_info;
  sql_info.set_sql(query);
  sql_info.set_datasource_id(datasource_id_);
  auto stream_reader = stream->GetReader(sql_info);
  YACL_ENFORCE(stream_reader != nullptr);
  return std::make_shared<DpChunkedResult>(std::move(stream_reader));
}

std::optional<arrow::ChunkedArrayVector> DpChunkedResult::Fetch() {
  std::shared_ptr<arrow::RecordBatch> batch;
  stream_reader_->Get(&batch);
  if (batch == nullptr || batch->num_rows() == 0) {
    return std::nullopt;
  }
  arrow::ChunkedArrayVector chunked_arrs;
  for (int i = 0; i < batch->num_columns(); ++i) {
    chunked_arrs.push_back(
        arrow::ChunkedArray::Make({batch->column(i)}).ValueOrDie());
  }
  return chunked_arrs;
}

}  // namespace scql::engine
