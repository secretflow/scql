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
class TrigonometricFunction : public Operator {
 public:
  static constexpr char kIn[] = "In";
  static constexpr char kOut[] = "Out";

 protected:
  // It will throw exception if validation fails
  void Validate(ExecContext* ctx) override;
  void Execute(ExecContext* ctx) override;

  void ExecuteInSecret(ExecContext* ctx);

  virtual void ExecuteInPlain(ExecContext* ctx) = 0;
  void ExecuteTrigonometricFunction(ExecContext* ctx, const std::string& func);
  virtual spu::Value ComputeOnSpu(spu::SPUContext* sctx,
                                  const spu::Value& value) = 0;
};

class Sine : public TrigonometricFunction {
 public:
  static const std::string kOpType;
  const std::string& Type() const override;
  void ExecuteInPlain(ExecContext* ctx) override;
  spu::Value ComputeOnSpu(spu::SPUContext* sctx,
                          const spu::Value& value) override;
};

class Cosine : public TrigonometricFunction {
 public:
  static const std::string kOpType;
  const std::string& Type() const override;
  void ExecuteInPlain(ExecContext* ctx) override;
  spu::Value ComputeOnSpu(spu::SPUContext* sctx,
                          const spu::Value& value) override;
};

class ACosine : public TrigonometricFunction {
 public:
  static const std::string kOpType;
  const std::string& Type() const override;
  void ExecuteInPlain(ExecContext* ctx) override;
  spu::Value ComputeOnSpu(spu::SPUContext* sctx,
                          const spu::Value& value) override;
};

}  // namespace scql::engine::op
