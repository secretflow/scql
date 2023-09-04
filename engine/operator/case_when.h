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

#include <string>

#include "engine/framework/exec.h"
#include "engine/framework/operator.h"

namespace scql::engine::op {

class CaseWhen : public Operator {
 public:
  static const std::string kOpType;

  static constexpr char kCond[] = "Condition";
  static constexpr char kValue[] = "Value";
  static constexpr char kValueElse[] = "ValueElse";
  static constexpr char kOut[] = "Out";
  const std::string& Type() const override;
  static std::vector<spu::Value> TransformConditionData(ExecContext* ctx);

 protected:
  void Validate(ExecContext* ctx) override;
  void Execute(ExecContext* ctx) override;

 private:
  void CaseWhenPrivate(ExecContext* ctx);
  void CaseWhenShare(ExecContext* ctx);
};

}  // namespace scql::engine::op