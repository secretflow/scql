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
#include "engine/operator/binary_base.h"

namespace scql::engine::op {

/// @brief Out = Not In
class Not : public Operator {
 public:
  static const std::string kOpType;

  static constexpr char kIn[] = "In";
  static constexpr char kOut[] = "Out";

  const std::string& Type() const override;

 protected:
  void Validate(ExecContext* ctx) override;
  void Execute(ExecContext* ctx) override;

  void ExecuteInPlain(ExecContext* ctx);
  void ExecuteInSecret(ExecContext* ctx);
};

class LogicalBase : public BinaryBase {
 protected:
  void ValidateIoDataTypes(ExecContext* ctx) override;
};

class LogicalAnd : public LogicalBase {
 public:
  static const std::string kOpType;
  const std::string& Type() const override;

 protected:
  spu::Value ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& lhs,
                          const spu::Value& rhs) override;

  TensorPtr ComputeInPlain(const Tensor& lhs, const Tensor& rhs) override;
};

class LogicalOr : public LogicalBase {
 public:
  static const std::string kOpType;
  const std::string& Type() const override;

 protected:
  spu::Value ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& lhs,
                          const spu::Value& rhs) override;

  TensorPtr ComputeInPlain(const Tensor& lhs, const Tensor& rhs) override;
};

}  // namespace scql::engine::op