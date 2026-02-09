// Copyright 2025 Ant Group Co., Ltd.
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

#include "libspu/kernel/hal/public_helper.h"
#include "libspu/kernel/hlo/geometrical.h"
#include "libspu/kernel/hlo/group_by_agg.h"

#include "engine/framework/operator.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

class GroupSecretAggBase : public Operator {
 public:
  virtual ~GroupSecretAggBase() = default;

  static constexpr char kGroupNum[] = "GroupNum";
  static constexpr char kGroupId[] = "GroupId";
  static constexpr char kIn[] = "In";
  static constexpr char kOut[] = "Out";

  void Validate(ExecContext* ctx) override;
  void Execute(ExecContext* ctx) override;

 protected:
  virtual spu::kernel::hlo::AggFunc GetAggregationFunction() const = 0;
  virtual spu::Value HandleEmptyInput(spu::SPUContext* sctx,
                                      const spu::Value& in) {
    return in.clone();
  }
};

class GroupSecretSum : public GroupSecretAggBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override { return kOpType; }

 protected:
  spu::kernel::hlo::AggFunc GetAggregationFunction() const override;
};

// FIXME: currently GroupSecretAvg default outputs float32 tensor, but
// Interpreter expects float64 tensor.
class GroupSecretAvg : public GroupSecretAggBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override { return kOpType; }

 protected:
  spu::kernel::hlo::AggFunc GetAggregationFunction() const override;
  spu::Value HandleEmptyInput(spu::SPUContext* sctx,
                              const spu::Value& in) override;
};

}  // namespace scql::engine::op
