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

#include "engine/operator/unary_base.h"

namespace scql::engine::op {
#define DEFINE_UNARY_CLASS(className)                       \
  class className : public UnaryBase {                      \
   public:                                                  \
    const std::string& Type() const override;               \
    static const std::string kOpType;                       \
                                                            \
   protected:                                               \
    TensorPtr ComputeInPlain(const Tensor& in) override;    \
    spu::Value ComputeOnSpu(spu::SPUContext* sctx,          \
                            const spu::Value& in) override; \
  };

DEFINE_UNARY_CLASS(Abs);
DEFINE_UNARY_CLASS(Ceil);
DEFINE_UNARY_CLASS(Floor);
DEFINE_UNARY_CLASS(Round);
DEFINE_UNARY_CLASS(Radians);
DEFINE_UNARY_CLASS(Degrees);
DEFINE_UNARY_CLASS(Ln);
DEFINE_UNARY_CLASS(Log10);
DEFINE_UNARY_CLASS(Log2);
DEFINE_UNARY_CLASS(Sqrt);
DEFINE_UNARY_CLASS(Exp);

#undef DEFINE_UNARY_CLASS
}  // namespace scql::engine::op