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

/// @brief base class for binary operators.
/// <Out> = <Left> op <Right>
///
/// Input and Output status relationship:
///   - If <Left> is private, <Right> could be private or public, <Out> status
///   would be private too.
///   - If <Left> is secret, <Right> could be secret or public, <Out> status
///   would be secret too.
///   - If <Left> is public, <Right> could be private or secret, <Out> status
///   would be the same as <Right>.
class BinaryBase : public Operator {
 public:
  static constexpr char kInLeft[] = "Left";
  static constexpr char kInRight[] = "Right";
  static constexpr char kOut[] = "Out";

 protected:
  void Validate(ExecContext* ctx) override;

  void Execute(ExecContext* ctx) override;

  // validate input/output data types
  virtual void ValidateIoDataTypes(ExecContext* ctx) = 0;

  void ExecuteInSecret(ExecContext* ctx);

  void ExecuteInPlain(ExecContext* ctx);

  virtual TensorPtr ComputeInPlain(const Tensor& lhs, const Tensor& rhs) = 0;

  virtual spu::Value ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& lhs,
                                  const spu::Value& rhs) = 0;

  // propagate nulls for arithmetic op
  static spu::Value PropagateNulls(spu::SPUContext* sctx, const spu::Value& lhs,
                                   const spu::Value& rhs);
};

}  // namespace scql::engine::op