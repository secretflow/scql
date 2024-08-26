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

#include "engine/datasource/datasource_adaptor.h"

#include "arrow/compute/cast.h"
#include "yacl/base/exception.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor_constructor.h"
#include "engine/core/type.h"

namespace scql::engine {
std::vector<TensorPtr> DatasourceAdaptor::ConvertDataTypeToExpected(
    const std::shared_ptr<spdlog::logger>& logger,
    std::vector<TensorPtr>& tensors,
    const std::vector<ColumnDesc>& expected_outputs) {
  YACL_ENFORCE_EQ(tensors.size(), expected_outputs.size(),
                  "query result column size={} not equal to expected size={}",
                  tensors.size(), expected_outputs.size());
  std::vector<TensorPtr> converted_tensors;
  for (size_t i = 0; i < tensors.size(); ++i) {
    auto tensor = tensors[i];
    if ((expected_outputs[i].dtype != pb::PrimitiveDataType::DATETIME &&
         expected_outputs[i].dtype != pb::PrimitiveDataType::TIMESTAMP &&
         FromArrowDataType(tensor->ArrowType()) != expected_outputs[i].dtype) ||
        // scql don't support utf8, which limits size < 2G, so should be
        // converted to large_utf8() to support large scale
        tensors[i]->ArrowType() == arrow::utf8()) {
      auto chunked_arr = tensor->ToArrowChunkedArray();
      YACL_ENFORCE(chunked_arr, "get column(idx={}) from table failed", i);
      auto to_type = ToArrowDataType(expected_outputs[i].dtype);
      YACL_ENFORCE(to_type, "unsupported column data type {}",
                   fmt::underlying(expected_outputs[i].dtype));
      SPDLOG_LOGGER_WARN(logger, "arrow type mismatch, convert from {} to {}",
                         chunked_arr->type()->ToString(), to_type->ToString());
      arrow::Datum cast_result;
      ASSIGN_OR_THROW_ARROW_STATUS(cast_result,
                                   arrow::compute::Cast(chunked_arr, to_type));
      chunked_arr = cast_result.chunked_array();
      tensor = TensorFrom(std::move(chunked_arr));
    }
    converted_tensors.push_back(std::move(tensor));
  }
  return converted_tensors;
}
}  // namespace scql::engine