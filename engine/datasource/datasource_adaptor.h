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

#include <memory>
#include <string>

#include "engine/core/tensor.h"

#include "api/core.pb.h"

namespace scql::engine {

enum ConnectionType {
  Unknown = 0,
  Short = 1,
  Pooled = 2,
};

/// @brief ColumnDesc contains column metadata information.
struct ColumnDesc {
  std::string name;             // column name
  pb::PrimitiveDataType dtype;  // column data type

  ColumnDesc(std::string name, pb::PrimitiveDataType dtype)
      : name(std::move(name)), dtype(dtype) {}
};

class DatasourceAdaptor {
 public:
  virtual ~DatasourceAdaptor() = default;

  // ExecQuery execute query,
  std::vector<TensorPtr> ExecQuery(
      const std::string& query,
      const std::vector<ColumnDesc>& expected_outputs) {
    auto tensors = GetQueryResult(query);
    return ConvertDataTypeToExpected(tensors, expected_outputs);
  }

 private:
  // Get result from data source
  virtual std::vector<TensorPtr> GetQueryResult(const std::string& query) = 0;
  // It will complain if the actual outputs not matched with expected_outputs.
  std::vector<TensorPtr> ConvertDataTypeToExpected(
      std::vector<TensorPtr>& tensors,
      const std::vector<ColumnDesc>& expected_outputs);
};

}  // namespace scql::engine