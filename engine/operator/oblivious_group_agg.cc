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

#include <cstdint>
#include <utility>
#include <vector>

#include "engine/util/prefix_sum.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

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

  auto* symbols = ctx->GetSession()->GetDeviceSymbols();
  auto* sctx = ctx->GetSession()->GetSpuContext();

  const auto& group = ctx->GetInput(kGroup)[0];
  auto group_value = TransferGroupMask(
      sctx,
      symbols->getVar(util::SpuVarNameEncoder::GetValueName(group.name())));

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

  return util::Scan(
      sctx, value_hat, group,
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
      sctx,
      spu::kernel::hlo::Constant(sctx, static_cast<int64_t>(1), value.shape()));

  return util::Scan(
      sctx, ones, group,
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

  return util::Scan(
      sctx, value, group,
      [&](const spu::Value& lhs_v, const spu::Value& lhs_gm,
          const spu::Value& rhs_v, const spu::Value& rhs_gm) {
        spu::Value new_v = spu::kernel::hlo::Select(
            sctx, lhs_gm, spu::kernel::hlo::Max(sctx, lhs_v, rhs_v), lhs_v);
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

  return util::Scan(
      sctx, value, group,
      [&](const spu::Value& lhs_v, const spu::Value& lhs_gm,
          const spu::Value& rhs_v, const spu::Value& rhs_gm) {
        spu::Value new_v = spu::kernel::hlo::Select(
            sctx, lhs_gm, spu::kernel::hlo::Min(sctx, lhs_v, rhs_v), lhs_v);
        spu::Value new_gm = spu::kernel::hlo::Mul(sctx, lhs_gm, rhs_gm);

        return std::make_pair(new_v, new_gm);
      });
}

const std::string ObliviousPercentRank::kOpType("ObliviousPercentRank");
const std::string& ObliviousPercentRank::Type() const { return kOpType; }

spu::Value ObliviousPercentRank::CalculateResult(spu::SPUContext* sctx,
                                                 const spu::Value& value,
                                                 const spu::Value& partition) {
  auto op = ObliviousGroupCount();
  spu::Value row_number = op.CalculateResult(sctx, value, partition);
  spu::Value reverted_partition = RevertGroupMaskTransfer(sctx, partition);
  spu::Value reversed_mark = spu::kernel::hlo::Sub(
      sctx, spu::kernel::hlo::Constant(sctx, 1, partition.shape()),
      reverted_partition);
  std::vector<spu::Value> group_count =
      util::ExpandGroupValueReversely(sctx, {row_number}, reversed_mark);
  spu::Value float_row_number = spu::kernel::hlo::Cast(
      sctx, row_number, row_number.vtype(), spu::DataType::DT_F64);
  return spu::kernel::hlo::Div(sctx, float_row_number, group_count[0]);
}
};  // namespace scql::engine::op
