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

#include "libspu/kernel/hal/constants.h"
#include "libspu/kernel/hlo/basic_binary.h"
#include "libspu/kernel/hlo/basic_ternary.h"
#include "libspu/kernel/hlo/casting.h"
#include "libspu/kernel/hlo/const.h"
#include "libspu/kernel/hlo/geometrical.h"

#include "engine/framework/operator.h"

namespace scql::engine::op {
namespace {

int64_t RowCount(const spu::Value& value) {
  return value.shape().size() > 0 ? value.shape()[0] : value.numel();
}

// IN  GroupMask: 1 means end of the group while 0 means other conditions.
// OUT GroupMask: 0 means start of the group while 1 means other conditions.
// e.g. IN: [0, 0, 1, 0, 0, 1, 0, 1, 0, 1]
//      OUT:[0, 1, 1, 0, 1, 1, 0, 1, 0, 1]
spu::Value TransferGroupMask(spu::SPUContext* ctx, const spu::Value& in) {
  const int64_t row_cnt = in.shape()[0];
  spu::Value in_not = spu::kernel::hlo::Sub(
      ctx, spu::kernel::hlo::Constant(ctx, 1, in.shape()), in);

  auto zero = spu::kernel::hal::zeros(ctx, in_not.dtype(), {1});
  if (in_not.vtype() == spu::VIS_SECRET) {
    zero = spu::kernel::hlo::Seal(ctx, zero);
  }

  return spu::kernel::hlo::Concatenate(
      ctx, {zero, spu::kernel::hlo::Slice(ctx, in_not, {0}, {row_cnt - 1}, {})},
      0);
}

// the inverse operation of the TransferGroupMask.
// IN: [0, 1, 1, 0, 1, 1, 0, 1, 0, 1], must be in the format of
// TransferGroupMask output. OUT:[0, 0, 1, 0, 0, 1, 0, 1, 0, 1]
spu::Value RevertGroupMaskTransfer(spu::SPUContext* ctx, const spu::Value& in) {
  const int64_t row_cnt = in.shape()[0];
  auto zero = spu::kernel::hal::zeros(ctx, in.dtype(), {1});
  if (in.vtype() == spu::VIS_SECRET) {
    zero = spu::kernel::hlo::Seal(ctx, zero);
  }

  auto cut = spu::kernel::hlo::Slice(ctx, in, {1}, {row_cnt}, {});
  auto in_not = spu::kernel::hlo::Concatenate(ctx, {cut, zero}, 0);
  return spu::kernel::hlo::Sub(
      ctx, spu::kernel::hlo::Constant(ctx, 1, in.shape()), in_not);
}

}  // namespace

class ObliviousGroupAggBase : public Operator {
 public:
  static constexpr char kGroup[] = "Group";
  static constexpr char kIn[] = "In";
  static constexpr char kOut[] = "Out";

 public:
  virtual spu::Value HandleEmptyInput(const spu::Value& in) { return in; }

  virtual spu::Value CalculateResult(spu::SPUContext* sctx,
                                     const spu::Value& value,
                                     const spu::Value& group_value) = 0;

 protected:
  void Validate(ExecContext* ctx) override;

  void Execute(ExecContext* ctx) override;
  virtual void InitAttribute(ExecContext* ctx) {};
};

class ObliviousGroupSum : public ObliviousGroupAggBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 public:
  spu::Value HandleEmptyInput(const spu::Value& in) override {
    // Sum(bool tensor) should return int64
    if (in.dtype() == spu::DT_I1) {
      return in.clone().setDtype(spu::DT_I64, true);
    } else {
      return in;
    }
  }

  spu::Value CalculateResult(spu::SPUContext* sctx, const spu::Value& value,
                             const spu::Value& group_value) override;
};

class ObliviousGroupCount : public ObliviousGroupAggBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 public:
  spu::Value HandleEmptyInput(const spu::Value& in) override {
    return in.clone().setDtype(spu::DT_I64, true);
  }

  spu::Value CalculateResult(spu::SPUContext* sctx, const spu::Value& value,
                             const spu::Value& group_value) override;
};

class ObliviousGroupAvg : public ObliviousGroupAggBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 public:
  spu::Value HandleEmptyInput(const spu::Value& in) override {
    return in.clone().setDtype(spu::DT_F64, true);
  }

  spu::Value CalculateResult(spu::SPUContext* sctx, const spu::Value& value,
                             const spu::Value& group_value) override;
};

class ObliviousGroupMax : public ObliviousGroupAggBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 public:
  spu::Value CalculateResult(spu::SPUContext* sctx, const spu::Value& value,
                             const spu::Value& group_value) override;
};

class ObliviousGroupMin : public ObliviousGroupAggBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 public:
  spu::Value CalculateResult(spu::SPUContext* sctx, const spu::Value& value,
                             const spu::Value& group_value) override;
};
// TODO(jingshi) : Add ObliviousGroupMedian.

class ObliviousPercentRank : public ObliviousGroupAggBase {
 public:
  static const std::string kOpType;
  const std::string& Type() const override;

 public:
  spu::Value HandleEmptyInput(const spu::Value& in) override {
    return in.clone().setDtype(spu::DT_F64, true);
  }

  spu::Value CalculateResult(spu::SPUContext* sctx, const spu::Value& value,
                             const spu::Value& partition_value) override;
};

class ObliviousPercentileDisc : public ObliviousGroupAggBase {
 public:
  static const std::string kOpType;
  static constexpr char kPercent[] = "percent";
  const std::string& Type() const override;

 public:
  spu::Value CalculateResult(spu::SPUContext* sctx, const spu::Value& value,
                             const spu::Value& group_value) override;

 protected:
  void InitAttribute(ExecContext* ctx) override;

 private:
  double percent_ = -1;
};
}  // namespace scql::engine::op
