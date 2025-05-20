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

#include <future>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "spdlog/spdlog.h"

#include "engine/core/tensor.h"
#include "engine/core/tensor_constructor.h"
#include "engine/util/concurrent_queue.h"

#include "api/core.pb.h"

namespace scql::engine {

/// @brief ColumnDesc contains column metadata information.
struct ColumnDesc {
  std::string name;             // column name
  pb::PrimitiveDataType dtype;  // column data type

  ColumnDesc(std::string name, pb::PrimitiveDataType dtype)
      : name(std::move(name)), dtype(dtype) {}
};

class ChunkedResult {
 public:
  virtual ~ChunkedResult() = default;
  virtual std::optional<arrow::ChunkedArrayVector> Fetch() = 0;
  virtual std::shared_ptr<arrow::Schema> GetSchema() = 0;
};

class DatasourceAdaptor {
 public:
  virtual ~DatasourceAdaptor() = default;

  // ExecQuery execute query,
  std::vector<TensorPtr> ExecQuery(
      const std::shared_ptr<spdlog::logger>& logger, const std::string& query,
      const std::vector<ColumnDesc>& expected_outputs,
      const TensorBuildOptions& options = {});

  std::vector<TensorPtr> ExecQuery(
      const std::string& query, const std::vector<ColumnDesc>& expected_outputs,
      const TensorBuildOptions& options = {}) {
    return ExecQuery(spdlog::default_logger(), query, expected_outputs,
                     options);
  }

 private:
  // Send query and return schema
  virtual std::shared_ptr<ChunkedResult> SendQuery(
      const std::string& query) = 0;
  // It will complain if the actual outputs not matched with expected_outputs.
  static arrow::ChunkedArrayVector ConvertDataTypeToExpected(
      const std::shared_ptr<spdlog::logger>& logger,
      const arrow::ChunkedArrayVector& chunked_arrs,
      const std::vector<ColumnDesc>& expected_outputs);

  static std::vector<TensorPtr> CreateDiskTensor(
      const std::shared_ptr<spdlog::logger>& logger,
      const std::vector<ColumnDesc>& expected_outputs,
      const TensorBuildOptions& options,
      util::SimpleChannel<arrow::ChunkedArrayVector>& data_queue);

  static std::vector<TensorPtr> CreateMemTensor(
      const std::shared_ptr<spdlog::logger>& logger,
      const std::vector<ColumnDesc>& expected_outputs,
      [[maybe_unused]] const TensorBuildOptions& options,
      util::SimpleChannel<arrow::ChunkedArrayVector>& data_queue);
};

}  // namespace scql::engine
