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

/*
left: [1, 2, 3], right: ['a', 'b']
the result after replicate is
left output:    [ 1,  2,  3,  1,  2,  3]
right output:   ['a','a','a','b','b','b']
*/
class Replicate : public Operator {
 public:
  const std::string& Type() const override;
  static constexpr char kLeft[] = "Left";
  static constexpr char kRight[] = "Right";
  static constexpr char kLeftOut[] = "LeftOut";
  static constexpr char kRightOut[] = "RightOut";
  static constexpr char kInputPartyCodesAttr[] = "input_party_codes";
  static const std::string kOpType;

 protected:
  void Execute(ExecContext* ctx) override;
  void Validate(ExecContext* ctx) override;
  void BuildReplicate(ExecContext* ctx, const RepeatedPbTensor& inputs,
                      size_t dup_rows, const RepeatedPbTensor& outputs,
                      bool interleaving);

 private:
  static std::shared_ptr<arrow::ChunkedArray> ExtendArray(
      const std::shared_ptr<arrow::ChunkedArray>& array, int64_t extend_size,
      bool interleaving);
};
}  // namespace scql::engine::op