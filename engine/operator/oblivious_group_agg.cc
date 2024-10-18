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

#include "engine/operator/oblivious_group_agg.h"

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "absl/numeric/bits.h"
#include "libspu/kernel/hal/constants.h"
#include "libspu/kernel/hlo/basic_binary.h"
#include "libspu/kernel/hlo/basic_ternary.h"
#include "libspu/kernel/hlo/casting.h"
#include "libspu/kernel/hlo/const.h"
#include "libspu/kernel/hlo/geometrical.h"
#include "libspu/kernel/hlo/indexing.h"

#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

namespace {

int64_t RowCount(const spu::Value& value) {
  return value.shape().size() > 0 ? value.shape()[0] : value.numel();
}

using ScanFn = std::function<std::pair<spu::Value, spu::Value>(
    const spu::Value&, const spu::Value&, const spu::Value&,
    const spu::Value&)>;
using IndexTuple = std::pair<spu::Index, spu::Index>;
using ScanFnParallel = std::function<void(IndexTuple&)>;

// Implements the most naive version of prefix sum tree structure.
// Refer to
// "Circuit representation of a work-efficient 16-input parallel prefix sum"
// https://en.wikipedia.org/wiki/Prefix_sum#/media/File:Prefix_sum_16.svg
void ScanParallel(std::size_t n, const ScanFnParallel& parallel_fn) {
  size_t k = absl::bit_width(n - 1u);

  if (n < 2) {
    return;
  }

  for (size_t i = 1; i < (k + 1); i++) {
    IndexTuple it;
    for (size_t j = ((1 << i) - 1); j < n; j += (1 << i)) {
      it.first.emplace_back(j);
      it.second.emplace_back(j - (1 << (i - 1)));
    }
    if (!it.first.empty()) {
      parallel_fn(it);
    }
  }

  for (size_t i = (k - 1); i > 0; i--) {
    IndexTuple it;
    for (size_t j = (3 * (1 << (i - 1)) - 1); j < n; j += (1 << i)) {
      it.first.emplace_back(j);
      it.second.emplace_back(j - (1 << (i - 1)));
    }

    if (!it.first.empty()) {
      parallel_fn(it);
    }
  }
  return;
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

// Reference: "Scape: Scalable Collaborative Analytics System on Private
// Database with Malicious Security", Fig. 13
//
spu::Value Scan(spu::SPUContext* ctx, const spu::Value& origin_value,
                const spu::Value& origin_group_mask, const ScanFn& scan_fn) {
  const int64_t row_cnt = origin_value.shape()[0];
  spu::Value group_mask = TransferGroupMask(ctx, origin_group_mask);
  spu::Value value = origin_value.clone();
  auto parallel_scan_fn = [&](IndexTuple& index_tuple) {
    auto lhs_v = spu::kernel::hlo::LinearGather(ctx, value, index_tuple.first);
    auto lhs_gm =
        spu::kernel::hlo::LinearGather(ctx, group_mask, index_tuple.first);

    auto rhs_v = spu::kernel::hlo::LinearGather(ctx, value, index_tuple.second);
    auto rhs_gm =
        spu::kernel::hlo::LinearGather(ctx, group_mask, index_tuple.second);

    const auto [new_v, new_gm] = scan_fn(lhs_v, lhs_gm, rhs_v, rhs_gm);

    YACL_ENFORCE_EQ(value.dtype(), new_v.dtype());
    YACL_ENFORCE_EQ(group_mask.dtype(), new_gm.dtype());

    spu::kernel::hlo::LinearScatterInPlace(ctx, value, new_v,
                                           index_tuple.first);
    spu::kernel::hlo::LinearScatterInPlace(ctx, group_mask, new_gm,
                                           index_tuple.first);
  };
  ScanParallel(row_cnt, parallel_scan_fn);
  return value;
}

}  // namespace

void ObliviousGroupAggBase::Validate(ExecContext* ctx) {
  const auto& group = ctx->GetInput(kGroup);
  YACL_ENFORCE(group.size() == 1, "group size must be 1");
  const auto& inputs = ctx->GetInput(kIn);
  YACL_ENFORCE(inputs.size() > 0, "input size cannot be 0");
  const auto& outputs = ctx->GetOutput(kOut);
  YACL_ENFORCE(outputs.size() == inputs.size(),
               "outputs' size={} not equal to inputs' size={}", outputs.size(),
               inputs.size());

  YACL_ENFORCE(util::IsTensorStatusMatched(group[0], pb::TENSORSTATUS_SECRET),
               "group's status is not secret");
  YACL_ENFORCE(util::AreTensorsStatusMatched(inputs, pb::TENSORSTATUS_SECRET),
               "inputs' status are not all secret");
  YACL_ENFORCE(util::AreTensorsStatusMatched(outputs, pb::TENSORSTATUS_SECRET),
               "outputs' status are not all secret");
}

void ObliviousGroupAggBase::Execute(ExecContext* ctx) {
  const auto& input_pbs = ctx->GetInput(kIn);
  const auto& output_pbs = ctx->GetOutput(kOut);

  auto symbols = ctx->GetSession()->GetDeviceSymbols();
  auto sctx = ctx->GetSession()->GetSpuContext();

  const auto& group = ctx->GetInput(kGroup)[0];
  auto group_value =
      symbols->getVar(util::SpuVarNameEncoder::GetValueName(group.name()));

  for (int i = 0; i < input_pbs.size(); ++i) {
    auto value = symbols->getVar(
        util::SpuVarNameEncoder::GetValueName(input_pbs[i].name()));

    spu::Value result;
    if (RowCount(value) == 0) {
      result = HandleEmptyInput(value);
    } else {
      result = CalculateResult(sctx, value, group_value);
    }

    symbols->setVar(util::SpuVarNameEncoder::GetValueName(output_pbs[i].name()),
                    result);
  }
}

// ===========================
//   Sum impl
// ===========================

const std::string ObliviousGroupSum::kOpType("ObliviousGroupSum");

const std::string& ObliviousGroupSum::Type() const { return kOpType; }

spu::Value ObliviousGroupSum::CalculateResult(spu::SPUContext* sctx,
                                              const spu::Value& value,
                                              const spu::Value& group) {
  int64_t row_count = RowCount(value);
  YACL_ENFORCE(row_count == RowCount(group));

  spu::Value value_hat;

  // NOTE: hack for boolean value.
  if (value.dtype() == spu::DT_I1) {
    value_hat = spu::kernel::hlo::Cast(sctx, value, value.vtype(), spu::DT_I64);
  } else {
    value_hat = value;
  }

  return Scan(sctx, value_hat, group,
              [&](const spu::Value& lhs_v, const spu::Value& lhs_gm,
                  const spu::Value& rhs_v, const spu::Value& rhs_gm) {
                spu::Value new_v = spu::kernel::hlo::Add(
                    sctx, lhs_v, spu::kernel::hlo::Mul(sctx, lhs_gm, rhs_v));
                spu::Value new_gm = spu::kernel::hlo::Mul(sctx, lhs_gm, rhs_gm);

                return std::make_pair(new_v, new_gm);
              });
}

// ===========================
//   Count impl
// ===========================

const std::string ObliviousGroupCount::kOpType("ObliviousGroupCount");

const std::string& ObliviousGroupCount::Type() const { return kOpType; }

spu::Value ObliviousGroupCount::CalculateResult(spu::SPUContext* sctx,
                                                const spu::Value& value,
                                                const spu::Value& group) {
  int64_t row_count = RowCount(value);
  YACL_ENFORCE(row_count == RowCount(group));

  spu::Value ones = spu::kernel::hlo::Seal(
      sctx, spu::kernel::hlo::Constant(sctx, int64_t(1), value.shape()));

  return Scan(sctx, ones, group,
              [&](const spu::Value& lhs_v, const spu::Value& lhs_gm,
                  const spu::Value& rhs_v, const spu::Value& rhs_gm) {
                spu::Value new_v = spu::kernel::hlo::Add(
                    sctx, lhs_v, spu::kernel::hlo::Mul(sctx, lhs_gm, rhs_v));
                spu::Value new_gm = spu::kernel::hlo::Mul(sctx, lhs_gm, rhs_gm);

                return std::make_pair(new_v, new_gm);
              });
}

// ===========================
//   Avg impl
// ===========================

const std::string ObliviousGroupAvg::kOpType("ObliviousGroupAvg");

const std::string& ObliviousGroupAvg::Type() const { return kOpType; }

spu::Value ObliviousGroupAvg::CalculateResult(spu::SPUContext* sctx,
                                              const spu::Value& value,
                                              const spu::Value& group) {
  auto sum_result = ObliviousGroupSum().CalculateResult(sctx, value, group);
  auto count_result = ObliviousGroupCount().CalculateResult(sctx, value, group);

  if (sum_result.dtype() != spu::DT_F64) {
    const auto sum_result_f64 = spu::kernel::hlo::Cast(
        sctx, sum_result, sum_result.vtype(), spu::DT_F64);
    return spu::kernel::hlo::Div(sctx, sum_result_f64, count_result);
  } else {
    return spu::kernel::hlo::Div(sctx, sum_result, count_result);
  }
}

// ===========================
//   Max impl
// ===========================

const std::string ObliviousGroupMax::kOpType("ObliviousGroupMax");

const std::string& ObliviousGroupMax::Type() const { return kOpType; }

spu::Value ObliviousGroupMax::CalculateResult(spu::SPUContext* sctx,
                                              const spu::Value& value,
                                              const spu::Value& group) {
  int64_t row_count = RowCount(value);
  YACL_ENFORCE(row_count == RowCount(group));

  return Scan(sctx, value, group,
              [&](const spu::Value& lhs_v, const spu::Value& lhs_gm,
                  const spu::Value& rhs_v, const spu::Value& rhs_gm) {
                spu::Value new_v = spu::kernel::hlo::Select(
                    sctx, lhs_gm, spu::kernel::hlo::Max(sctx, lhs_v, rhs_v),
                    lhs_v);
                spu::Value new_gm = spu::kernel::hlo::Mul(sctx, lhs_gm, rhs_gm);

                return std::make_pair(new_v, new_gm);
              });
}

// ===========================
//   Min impl
// ===========================

const std::string ObliviousGroupMin::kOpType("ObliviousGroupMin");

const std::string& ObliviousGroupMin::Type() const { return kOpType; }

spu::Value ObliviousGroupMin::CalculateResult(spu::SPUContext* sctx,
                                              const spu::Value& value,
                                              const spu::Value& group) {
  int64_t row_count = RowCount(value);
  YACL_ENFORCE(row_count == RowCount(group));

  return Scan(sctx, value, group,
              [&](const spu::Value& lhs_v, const spu::Value& lhs_gm,
                  const spu::Value& rhs_v, const spu::Value& rhs_gm) {
                spu::Value new_v = spu::kernel::hlo::Select(
                    sctx, lhs_gm, spu::kernel::hlo::Min(sctx, lhs_v, rhs_v),
                    lhs_v);
                spu::Value new_gm = spu::kernel::hlo::Mul(sctx, lhs_gm, rhs_gm);

                return std::make_pair(new_v, new_gm);
              });
}

};  // namespace scql::engine::op
