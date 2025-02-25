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

/// @brief Idea from Scape: https://ieeexplore.ieee.org/document/9835540/
/// Warn: for simplicity, the intersection of join are revealed
class SecretJoin : public Operator {
 public:
  static const std::string kOpType;

  static constexpr char kLeftKey[] = "LeftKey";
  static constexpr char kRightKey[] = "RightKey";
  static constexpr char kLeftPayload[] = "Left";
  static constexpr char kRightPayload[] = "Right";
  static constexpr char kOutLeft[] = "LeftOutput";
  static constexpr char kOutRight[] = "RightOutput";

  const std::string& Type() const override;

 protected:
  void Validate(ExecContext* ctx) override;

  void Execute(ExecContext* ctx) override;

 private:
  void SetEmptyResult(ExecContext* ctx);

  std::tuple<std::vector<spu::Value>, std::vector<spu::Value>> MergeInputs(
      ExecContext* ctx);

  std::tuple<spu::Value, spu::Value, spu::Value> CountKeyGroup(
      spu::SPUContext* sctx, const std::vector<spu::Value>& keys,
      const spu::Value& item_origin_mark);
};

}  // namespace scql::engine::op