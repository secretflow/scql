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

#include "engine/operator/vertical_group_agg.h"

#include "absl/numeric/bits.h"
#include "libspu/kernel/hal/constants.h"
#include "libspu/kernel/hlo/basic_binary.h"
#include "libspu/kernel/hlo/basic_ternary.h"
#include "libspu/kernel/hlo/casting.h"
#include "libspu/kernel/hlo/const.h"
#include "libspu/kernel/hlo/geometrical.h"
#include "libspu/kernel/hlo/indexing.h"
#include "libspu/kernel/hlo/reduce.h"

#include "engine/operator/group_agg_helper.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

void VerticalGroupAggBase::Validate(ExecContext* ctx) {
  const auto& group = ctx->GetInput(kGroup);
  YACL_ENFORCE(group.size() == 1, "group size must be 1");
  const auto& inputs = ctx->GetInput(kIn);
  YACL_ENFORCE(inputs.size() > 0, "input size cannot be 0");
  const auto& outputs = ctx->GetOutput(kOut);
  YACL_ENFORCE(outputs.size() == inputs.size(),
               "outputs' size={} not equal to inputs' size={}", outputs.size(),
               inputs.size());

  YACL_ENFORCE(util::IsTensorStatusMatched(group[0], pb::TENSORSTATUS_PUBLIC),
               "group's status is not secret");
  YACL_ENFORCE(util::AreTensorsStatusMatched(inputs, pb::TENSORSTATUS_SECRET),
               "inputs' status are not all secret");
  YACL_ENFORCE(util::AreTensorsStatusMatched(outputs, pb::TENSORSTATUS_SECRET),
               "outputs' status are not all secret");
}

void VerticalGroupAggBase::Execute(ExecContext* ctx) {
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

const std::string VerticalGroupSum::kOpType("VerticalGroupSum");

const std::string& VerticalGroupSum::Type() const { return kOpType; }

spu::Value VerticalGroupSum::CalculateResult(spu::SPUContext* sctx,
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

  std::vector<spu::Value> single_output_elements;
  int start_idx = 0;
  for (int k = 0; k < row_count; ++k) {
    if (group.data().at<int>({k}) == 1) {
      auto reducer = [&](absl::Span<spu::Value const> lhs,
                         absl::Span<spu::Value const> rhs) {
        std::vector<spu::Value> out;
        std::transform(
            lhs.begin(), lhs.end(), rhs.begin(), std::back_inserter(out),
            [sctx](const spu::Value& lhs, const spu::Value& rhs) -> spu::Value {
              return spu::kernel::hlo::Add(sctx, lhs, rhs);
            });
        return out;
      };

      auto out_value = spu::kernel::hlo::Reduce(
          sctx,
          std::vector<spu::Value>{spu::kernel::hlo::Slice(
              sctx, value_hat, {start_idx}, {k + 1}, {})},
          std::vector<spu::Value>{}, spu::Axes{0}, reducer, true);
      single_output_elements.emplace_back(out_value[0]);
      start_idx = k + 1;
    }
  }

  return spu::kernel::hlo::Concatenate(sctx, single_output_elements, 0);
}

// ===========================
//   Count impl
// ===========================

const std::string VerticalGroupCount::kOpType("VerticalGroupCount");

const std::string& VerticalGroupCount::Type() const { return kOpType; }

spu::Value VerticalGroupCount::CalculateResult(spu::SPUContext* sctx,
                                               const spu::Value& value,
                                               const spu::Value& group) {
  int64_t row_count = RowCount(value);
  YACL_ENFORCE(row_count == RowCount(group));

  std::vector<spu::Value> single_output_elements;
  int64_t cnt = 0;
  for (int k = 0; k < row_count; ++k) {
    cnt++;
    if (group.data().at<int>({k}) == 1) {
      single_output_elements.emplace_back(
          spu::kernel::hlo::Constant(sctx, int64_t(cnt), {1}));
      cnt = 0;
    }
  }

  return spu::kernel::hlo::Concatenate(sctx, single_output_elements, 0);
}

// ===========================
//   Avg impl
// ===========================

const std::string VerticalGroupAvg::kOpType("VerticalGroupAvg");

const std::string& VerticalGroupAvg::Type() const { return kOpType; }

spu::Value VerticalGroupAvg::CalculateResult(spu::SPUContext* sctx,
                                             const spu::Value& value,
                                             const spu::Value& group) {
  auto sum_result = VerticalGroupSum().CalculateResult(sctx, value, group);
  auto count_result = VerticalGroupCount().CalculateResult(sctx, value, group);

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

const std::string VerticalGroupMax::kOpType("VerticalGroupMax");

const std::string& VerticalGroupMax::Type() const { return kOpType; }

spu::Value VerticalGroupMax::CalculateResult(spu::SPUContext* sctx,
                                             const spu::Value& value,
                                             const spu::Value& group) {
  int64_t row_count = RowCount(value);
  YACL_ENFORCE(row_count == RowCount(group));

  std::vector<spu::Value> single_output_elements;
  for (int k = 0; k < row_count; ++k) {
    if (group.data().at<int>({k}) == 1) {
      // the value is sorted before max, so just slice the last item as the
      // result for each group
      single_output_elements.emplace_back(
          spu::kernel::hlo::Slice(sctx, value, {k}, {k + 1}, {}));
    }
  }

  return spu::kernel::hlo::Concatenate(sctx, single_output_elements, 0);
}

// ===========================
//   Min impl
// ===========================

const std::string VerticalGroupMin::kOpType("VerticalGroupMin");

const std::string& VerticalGroupMin::Type() const { return kOpType; }

spu::Value VerticalGroupMin::CalculateResult(spu::SPUContext* sctx,
                                             const spu::Value& value,
                                             const spu::Value& group) {
  int64_t row_count = RowCount(value);
  YACL_ENFORCE(row_count == RowCount(group));

  std::vector<spu::Value> single_output_elements;
  for (int k = 0; k < row_count; ++k) {
    if (k == 0 || group.data().at<int>({k - 1}) == 1) {
      // the value is sorted before min, so just slice the first item as the
      // result for each group
      single_output_elements.emplace_back(
          spu::kernel::hlo::Slice(sctx, value, {k}, {k + 1}, {}));
    }
  }

  return spu::kernel::hlo::Concatenate(sctx, single_output_elements, 0);
}

}  // namespace scql::engine::op