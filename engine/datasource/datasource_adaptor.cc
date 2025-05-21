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

#include <cstddef>
#include <future>
#include <memory>
#include <vector>

#include "arrow/compute/cast.h"
#include "yacl/base/exception.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor_constructor.h"
#include "engine/core/type.h"
#include "engine/util/filepath_helper.h"
#include "engine/util/tensor_util.h"

namespace scql::engine {

// ExecQuery execute query,
std::vector<TensorPtr> DatasourceAdaptor::CreateDiskTensor(
    const std::shared_ptr<spdlog::logger>& logger,
    const std::vector<ColumnDesc>& expected_outputs,
    const TensorBuildOptions& options,
    util::SimpleChannel<arrow::ChunkedArrayVector>& data_queue) {
  std::vector<TensorPtr> tensors(expected_outputs.size());
  std::vector<std::shared_ptr<TensorWriter>> writers(expected_outputs.size());
  bool has_data = false;
  auto data_arrs = data_queue.Pop();
  while (data_arrs.has_value()) {
    auto converted_arrs =
        ConvertDataTypeToExpected(logger, data_arrs.value(), expected_outputs);
    if (!has_data) {
      has_data = true;
      for (size_t i = 0; i < expected_outputs.size(); ++i) {
        auto column_file_path = util::CreateDirWithRandSuffix(
            options.dump_dir, expected_outputs[i].name);
        auto writer = std::make_shared<TensorWriter>(
            expected_outputs[i].name, converted_arrs[i]->type(),
            column_file_path, options.max_row_num_one_file);
        writers[i] = writer;
      }
    }
    for (size_t i = 0; i < converted_arrs.size(); i++) {
      writers[i]->WriteBatch(*converted_arrs[i]);
    }
    data_arrs = data_queue.Pop();
  }
  if (!has_data) {
    // create empty tensor
    for (size_t i = 0; i < expected_outputs.size(); ++i) {
      tensors[i] = std::make_shared<MemTensor>(
          arrow::ChunkedArray::Make({},
                                    ToArrowDataType(expected_outputs[i].dtype))
              .ValueOrDie());
    }
  } else {
    for (size_t i = 0; i < expected_outputs.size(); ++i) {
      writers[i]->Finish(&tensors[i]);
    }
  }
  return tensors;
}

std::vector<TensorPtr> DatasourceAdaptor::CreateMemTensor(
    const std::shared_ptr<spdlog::logger>& logger,
    const std::vector<ColumnDesc>& expected_outputs,
    [[maybe_unused]] const TensorBuildOptions& options,
    util::SimpleChannel<arrow::ChunkedArrayVector>& data_queue) {
  std::vector<TensorPtr> tensors(expected_outputs.size());
  std::vector<std::shared_ptr<arrow::DataType>> types(expected_outputs.size());
  for (size_t i = 0; i < expected_outputs.size(); ++i) {
    types[i] = ToArrowDataType(expected_outputs[i].dtype);
  }
  std::vector<arrow::ChunkedArrayVector> result_arrs(expected_outputs.size());
  auto data_arrs = data_queue.Pop();
  while (data_arrs.has_value()) {
    auto converted_arrs =
        ConvertDataTypeToExpected(logger, data_arrs.value(), expected_outputs);
    for (size_t i = 0; i < converted_arrs.size(); i++) {
      // reset data type due to data type converted by
      // ConvertDataTypeToExpected
      types[i] = converted_arrs[i]->type();
      result_arrs[i].push_back(converted_arrs[i]);
    }
    data_arrs = data_queue.Pop();
  }
  for (size_t i = 0; i < result_arrs.size(); i++) {
    arrow::ArrayVector arrays;
    for (const auto& arrs : result_arrs[i]) {
      arrays.insert(arrays.end(), arrs->chunks().begin(), arrs->chunks().end());
    }
    tensors[i] = std::make_shared<MemTensor>(
        arrow::ChunkedArray::Make(arrays, types[i]).ValueOrDie());
  }
  return tensors;
}

std::vector<TensorPtr> DatasourceAdaptor::ExecQuery(
    const std::shared_ptr<spdlog::logger>& logger, const std::string& query,
    const std::vector<ColumnDesc>& expected_outputs,
    const TensorBuildOptions& options) {
  std::vector<TensorPtr> tensors;
  constexpr size_t data_queue_capacity = 1;
  util::SimpleChannel<arrow::ChunkedArrayVector> data_queue(
      data_queue_capacity);
  auto result = SendQuery(query);
  auto f = std::async(std::launch::async, [&] {
    try {
      while (true) {
        auto data_arrays = result->Fetch();
        if (!data_arrays.has_value()) {
          data_queue.Close();
          break;
        }
        data_queue.Push(data_arrays.value());
      }
    } catch (const std::exception& e) {
      // close queue to avoid other thread be blocked forever
      data_queue.Close();
      throw e;
    }
  });
  if (options.dump_to_disk) {
    // write data to disk
    tensors = CreateDiskTensor(logger, expected_outputs, options, data_queue);
  } else {
    // create in-memory tensor
    tensors = CreateMemTensor(logger, expected_outputs, options, data_queue);
  }
  f.get();

  return tensors;
}

arrow::ChunkedArrayVector DatasourceAdaptor::ConvertDataTypeToExpected(
    const std::shared_ptr<spdlog::logger>& logger,
    const arrow::ChunkedArrayVector& chunked_arrs,
    const std::vector<ColumnDesc>& expected_outputs) {
  YACL_ENFORCE_EQ(chunked_arrs.size(), expected_outputs.size(),
                  "query result column size={} not equal to expected size={}",
                  chunked_arrs.size(), expected_outputs.size());
  arrow::ChunkedArrayVector result_chunked_arrs;
  for (size_t i = 0; i < chunked_arrs.size(); ++i) {
    auto chunked_arr = chunked_arrs[i];
    if ((expected_outputs[i].dtype != pb::PrimitiveDataType::DATETIME &&
         expected_outputs[i].dtype != pb::PrimitiveDataType::TIMESTAMP &&
         FromArrowDataType(chunked_arr->type()) != expected_outputs[i].dtype) ||
        // scql don't support utf8, which limits size < 2G, so should be
        // converted to large_utf8() to support large scale
        chunked_arr->type() == arrow::utf8()) {
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
    }
    if (arrow::is_temporal(chunked_arr->type()->id())) {
      auto tensor = util::ConvertDateTimeToInt64(chunked_arr);
      chunked_arr = tensor->ToArrowChunkedArray();
    }
    result_chunked_arrs.push_back(std::move(chunked_arr));
  }
  return result_chunked_arrs;
}
}  // namespace scql::engine
