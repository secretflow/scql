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

#pragma once

#include "engine/framework/operator.h"

namespace scql::engine::op {
class RankWindowBase : public Operator {
 public:
  static constexpr char kIn[] = "Key";
  static constexpr char kOut[] = "Out";
  static constexpr char kPartitionId[] = "PartitionId";
  static constexpr char kPartitionNum[] = "PartitionNum";
  static constexpr char kReverseAttr[] = "reverse";

 protected:
  uint32_t GetPartitionNum(ExecContext* ctx);
  std::shared_ptr<arrow::Array> GetPartitionId(ExecContext* ctx);
  TensorPtr BuildTensorFromScalarVector(
      const arrow::ScalarVector& scalars,
      std::shared_ptr<arrow::DataType> empty_type);

  virtual void RunWindowFunc(ExecContext* ctx,
                             std::shared_ptr<arrow::Table> input,
                             const std::vector<int64_t>& positions,
                             std::vector<int64_t>& window_result) = 0;
  void Validate(ExecContext* ctx) override;
  void Execute(ExecContext* ctx) override;
  std::shared_ptr<arrow::ChunkedArray> VectorToChunkedArray(
      const std::vector<int64_t>& vec);
  std::shared_ptr<arrow::Array> GetSortedIndices(
      ExecContext* ctx, std::shared_ptr<arrow::Table> input);
  std::vector<std::shared_ptr<arrow::ListArray>> GetPartitionedInputs(
      ExecContext* ctx, const arrow::UInt32Array* partition_id,
      uint32_t partition_num);
};

class RowNumber : public RankWindowBase {
 public:
  static const std::string kOpType;
  const std::string& Type() const override;

 protected:
  void RunWindowFunc(ExecContext* ctx, std::shared_ptr<arrow::Table> input,
                     const std::vector<int64_t>& positions,
                     std::vector<int64_t>& window_result) override;
};
}  // namespace scql::engine::op