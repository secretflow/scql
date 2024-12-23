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
class UnaryBase : public Operator {
 public:
  static constexpr const char kIn[] = "In";
  static constexpr const char kOut[] = "Out";

 protected:
  void Execute(ExecContext* ctx) override;
  void Validate(ExecContext* ctx) override;
  void ExecuteInPlain(ExecContext* ctx);
  void ExecuteInSecret(ExecContext* ctx);
  virtual TensorPtr ComputeInPlain(const Tensor& in) = 0;
  virtual spu::Value ComputeOnSpu(spu::SPUContext* sctx,
                                  const spu::Value& in) = 0;
};
}  // namespace scql::engine::op