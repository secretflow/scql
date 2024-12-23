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
#include <memory>
#include <vector>

#include "absl/strings/ascii.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/compute/cast.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "dataproxy_sdk/cc/data_proxy_stream.h"
#include "gflags/gflags.h"

#include "engine/core/tensor.h"
#include "engine/core/tensor_constructor.h"
#include "engine/exe/flags.h"

namespace scql::engine {

std::shared_ptr<arrow::ChunkedArray> Dataproxy::ConvertDatetimeToInt64(
    const std::shared_ptr<arrow::ChunkedArray>& array) {
  if (arrow::is_temporal(array->type()->id())) {
    auto result =
        arrow::compute::Cast(array, arrow::timestamp(arrow::TimeUnit::SECOND))
            .ValueOrDie();
    result = arrow::compute::Cast(result, arrow::int64()).ValueOrDie();
    return result.chunked_array();
  }
  return array;
}

std::vector<TensorPtr> DpAdaptor::GetQueryResult(
    const std::string& query, const TensorBuildOptions& options) {
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
  auto reader = stream->GetReader(sql_info);
  YACL_ENFORCE(reader != nullptr);
  arrow::RecordBatchVector batches;
  // TODO(xiaoyuan): supporting streaming mode by writing record batched to file
  while (true) {
    std::shared_ptr<arrow::RecordBatch> batch;
    reader->Get(&batch);
    if (batch == nullptr || batch->num_rows() == 0) {
      break;
    }
    batches.push_back(batch);
  }
  auto table = arrow::Table::FromRecordBatches(batches).ValueOrDie();
  std::vector<TensorPtr> tensors;
  for (const auto& col : table->columns()) {
    tensors.push_back(TensorFrom(Dataproxy::ConvertDatetimeToInt64(col)));
  }
  return tensors;
}

}  // namespace scql::engine
