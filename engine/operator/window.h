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

#include "arrow/builder.h"

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
  static uint32_t GetPartitionNum(ExecContext* ctx);
  static std::shared_ptr<arrow::Array> GetPartitionId(ExecContext* ctx);
  TensorPtr BuildTensorFromScalarVector(
      const arrow::ScalarVector& scalars,
      std::shared_ptr<arrow::DataType> empty_type);

  virtual void RunWindowFunc(ExecContext* ctx,
                             std::shared_ptr<arrow::Table> input,
                             const std::vector<int64_t>& positions) = 0;
  void Validate(ExecContext* ctx) override;
  void Execute(ExecContext* ctx) override;
  static std::shared_ptr<arrow::Array> GetSortedIndices(
      ExecContext* ctx, const std::shared_ptr<arrow::Table>& input);
  static std::vector<std::shared_ptr<arrow::ListArray>> GetPartitionedInputs(
      ExecContext* ctx, const arrow::UInt32Array* partition_id,
      uint32_t partition_num);
  virtual std::shared_ptr<arrow::ChunkedArray> GetWindowResult() = 0;
  virtual void ExecuteInSecret(ExecContext* ctx) = 0;
  void ExecuteInPlain(ExecContext* ctx);
  virtual void ReserveWindowResult(size_t size) = 0;

  template <typename T>
  std::shared_ptr<arrow::ChunkedArray> VectorToChunkedArray(
      std::vector<T>& vec) {
    using BuilderType = typename arrow::CTypeTraits<T>::BuilderType;
    BuilderType builder;

    arrow::Status status;
    status = builder.Reserve(vec.size());
    YACL_ENFORCE(status.ok(), "Reserve memory failed");

    for (const auto& value : vec) {
      auto status = builder.Append(value);
      YACL_ENFORCE(status.ok(), "Append value failed");
    }

    std::shared_ptr<arrow::Array> chunk;
    status = builder.Finish(&chunk);
    YACL_ENFORCE(status.ok(), "Finish array failed");

    std::vector<std::shared_ptr<arrow::Array>> chunks;
    chunks.push_back(chunk);
    return std::make_shared<arrow::ChunkedArray>(chunks);
  }

  template <typename ValueType>
  void ProcessResults(const int length,
                      const std::unordered_map<int64_t, ValueType>& rank_map,
                      const std::vector<int64_t>& positions,
                      std::vector<ValueType>& window_results) {
    YACL_ENFORCE(length == (int64_t)positions.size(),
                 "position vector size not equal to that of input value");
    for (int64_t i = 0; i < length; ++i) {
      YACL_ENFORCE(rank_map.find(i) != rank_map.end(),
                   "failed to find the indice of position {}", i);
      YACL_ENFORCE(
          positions[i] >= 0 && positions[i] < (int64_t)window_results.size(),
          "position {} is out of range [0, {}", positions[i],
          window_results.size());
      window_results[positions[i]] = rank_map.at(i);
    }
  }

  template <typename ValueType, typename ValueGenerator>
  void BuildRankMap(arrow::Int64Array* int_array,
                    std::unordered_map<int64_t, ValueType>& rank_map,
                    ValueGenerator gen) {
    const int64_t total = int_array->length();
    int64_t rank = 1;
    for (int64_t i = 0; i < total; ++i) {
      int64_t key = int_array->Value(i);
      YACL_ENFORCE(rank_map.find(key) == rank_map.end(),
                   "duplicate row number");
      rank_map[key] = gen(rank, total);

      rank++;
    }
  }
};

class RowNumber : public RankWindowBase {
 public:
  static const std::string kOpType;
  const std::string& Type() const override;

 protected:
  void RunWindowFunc(ExecContext* ctx, std::shared_ptr<arrow::Table> input,
                     const std::vector<int64_t>& positions) override;
  void ExecuteInSecret(ExecContext* ctx) override {
    YACL_THROW("not support in secret mode");
  }
  std::shared_ptr<arrow::ChunkedArray> GetWindowResult() override {
    return VectorToChunkedArray(window_results_);
  }
  void ReserveWindowResult(size_t size) override {
    window_results_ = std::vector<int64_t>(size, 0);
  };

 private:
  std::vector<int64_t> window_results_;
};

class PercentRank : public RankWindowBase {
 public:
  static const std::string kOpType;
  const std::string& Type() const override;

 protected:
  void RunWindowFunc(ExecContext* ctx, std::shared_ptr<arrow::Table> input,
                     const std::vector<int64_t>& positions) override;
  void ExecuteInSecret(ExecContext* ctx) override {
    YACL_THROW("not support in secret mode");
  }
  std::shared_ptr<arrow::ChunkedArray> GetWindowResult() override {
    return VectorToChunkedArray(window_results_);
  };
  void ReserveWindowResult(size_t size) override {
    window_results_ = std::vector<double>(size, 0.0);
  };

 private:
  std::vector<double> window_results_;
};
}  // namespace scql::engine::op