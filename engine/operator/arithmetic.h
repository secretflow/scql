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

#include "engine/operator/binary_base.h"

namespace scql::engine::op {

/// @brief ArithmeticBase is base class of operator {+/-/*/DIV/INTDIV/MOD}
class ArithmeticBase : public BinaryBase {
 protected:
  void ValidateIoDataTypes(ExecContext* ctx) override;
};

class Add : public ArithmeticBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 protected:
  spu::Value ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& lhs,
                          const spu::Value& rhs) override;

  TensorPtr ComputeInPlain(const Tensor& lhs, const Tensor& rhs) override;
};

class Minus : public ArithmeticBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 protected:
  spu::Value ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& lhs,
                          const spu::Value& rhs) override;

  TensorPtr ComputeInPlain(const Tensor& lhs, const Tensor& rhs) override;
};

class Mul : public ArithmeticBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 protected:
  spu::Value ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& lhs,
                          const spu::Value& rhs) override;

  TensorPtr ComputeInPlain(const Tensor& lhs, const Tensor& rhs) override;
};

class Div : public ArithmeticBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 protected:
  spu::Value ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& lhs,
                          const spu::Value& rhs) override;

  TensorPtr ComputeInPlain(const Tensor& lhs, const Tensor& rhs) override;
};

class IntDiv : public ArithmeticBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 protected:
  spu::Value ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& lhs,
                          const spu::Value& rhs) override;

  TensorPtr ComputeInPlain(const Tensor& lhs, const Tensor& rhs) override;
};

class Mod : public ArithmeticBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 protected:
  spu::Value ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& lhs,
                          const spu::Value& rhs) override;

  TensorPtr ComputeInPlain(const Tensor& lhs, const Tensor& rhs) override;
};

}  // namespace scql::engine::op