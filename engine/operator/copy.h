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

#include "engine/framework/operator.h"

namespace scql::engine::op {

/// @brief Copy send private data from one party to another.
class Copy : public Operator {
 public:
  static const std::string kOpType;

  static constexpr char kIn[] = "In";
  static constexpr char kOut[] = "Out";
  static constexpr char kInputPartyCodesAttr[] = "input_party_codes";
  static constexpr char kOutputPartyCodesAttr[] = "output_party_codes";

  const std::string& Type() const override;

 protected:
  void Validate(ExecContext* ctx) override;
  void Execute(ExecContext* ctx) override;

 private:
  std::shared_ptr<arrow::Buffer> SerializeRecordBatch(
      std::shared_ptr<arrow::RecordBatch> batch);

  std::shared_ptr<arrow::Table> DeserializeTable(yacl::Buffer value);

  void InsertTensorsFromTable(ExecContext* ctx,
                              const RepeatedPbTensor& output_pbs,
                              std::shared_ptr<arrow::Table> table);

  size_t batch_size_ = 1000 * 1000;
};

}  // namespace scql::engine::op