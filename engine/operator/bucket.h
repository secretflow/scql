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

#include <utility>

#include "arrow/array/array_base.h"
#include "arrow/type_fwd.h"

#include "engine/framework/operator.h"
#include "engine/util/concurrent_queue.h"

namespace scql::engine::op {

/// @brief put data into buckets by hash of join key.
class Bucket : public Operator {
 public:
  static const std::string kOpType;

  static constexpr char kIn[] = "In";
  static constexpr char kJoinKey[] = "Key";
  static constexpr char kOut[] = "Out";
  static constexpr char kInputPartyCodesAttr[] = "input_party_codes";

  const std::string& Type() const override;

 protected:
  void Validate(ExecContext* ctx) override;
  void Execute(ExecContext* ctx) override;

 private:
  struct ReadResult {
    arrow::ArrayVector data_arrays;
    arrow::ChunkedArrayVector indice;

   public:
    ReadResult() = default;
    ReadResult(arrow::ArrayVector data_arrays, arrow::ChunkedArrayVector indice)
        : data_arrays(std::move(data_arrays)),

          indice(std::move(indice)) {}
  };
  static constexpr int kBatchParallelism = 10;
  util::SimpleChannel<ReadResult> read_queue_{kBatchParallelism};
  util::SimpleChannel<std::vector<arrow::ChunkedArrayVector>> write_queue_{
      kBatchParallelism};
};

}  // namespace scql::engine::op