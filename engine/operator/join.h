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

class Join : public Operator {
 public:
  static const std::string kOpType;
  // input/output names
  static constexpr char kInLeft[] = "Left";
  static constexpr char kInRight[] = "Right";
  static constexpr char kOutLeftJoinIndex[] = "LeftJoinIndex";
  static constexpr char kOutRightJoinIndex[] = "RightJoinIndex";
  // attributes
  static constexpr char kJoinTypeAttr[] = "join_type";
  static constexpr char kInputPartyCodesAttr[] = "input_party_codes";
  // join type
  static constexpr int64_t kInnerJoin = 0;

  const std::string& Type() const override;

 protected:
  void Validate(ExecContext* ctx) override;
  void Execute(ExecContext* ctx) override;

  std::vector<TensorPtr> GetJoinKeys(ExecContext* ctx, bool is_left);
  void SetJoinIndices(ExecContext* ctx, bool is_left, TensorPtr indices);
};

}  // namespace scql::engine::op