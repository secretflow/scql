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

#include "engine/operator/group_secret_agg.h"

#include "libspu/kernel/hlo/group_by_agg.h"

namespace scql::engine::op {

// GroupSecretAggBase implementation
void GroupSecretAggBase::Validate(ExecContext* ctx) {
  const auto& group_num = ctx->GetInput(kGroupNum);
  const auto& group_id = ctx->GetInput(kGroupId);
  const auto& inputs = ctx->GetInput(kIn);
  const auto& outputs = ctx->GetOutput(kOut);

  YACL_ENFORCE(group_num.size() == 1,
               "operator {} input GroupNum size {} must be 1", Type(),
               group_num.size());
  YACL_ENFORCE(group_id.size() == 1,
               "operator {} input GroupId size {} must be 1", Type(),
               group_id.size());
  YACL_ENFORCE(inputs.size() == 1, "operator {} input In size {} must be 1",
               Type(), inputs.size());
  YACL_ENFORCE(inputs.size() == outputs.size(),
               "operator {} output and input should have the same size",
               Type());

  YACL_ENFORCE(
      util::IsTensorStatusMatched(group_num[0], pb::TENSORSTATUS_PUBLIC));
  YACL_ENFORCE(
      util::IsTensorStatusMatched(group_id[0], pb::TENSORSTATUS_SECRET));
  YACL_ENFORCE(util::IsTensorStatusMatched(inputs[0], pb::TENSORSTATUS_SECRET));
  YACL_ENFORCE(
      util::IsTensorStatusMatched(outputs[0], pb::TENSORSTATUS_SECRET));

  YACL_ENFORCE(ctx->GetSession()
                   ->GetSpuContext()
                   ->config()
                   .experimental_enable_colocated_optimization,
               "{} must enable spu colocated optimization", Type());
}

void GroupSecretAggBase::Execute(ExecContext* ctx) {
  auto* sctx = ctx->GetSession()->GetSpuContext();
  auto group_id = ctx->GetInputValue(kGroupId);
  auto input = ctx->GetInputValue(kIn);
  auto group_num = ctx->GetInputValue(kGroupNum);
  if (group_id.numel() == 0) {
    auto result = HandleEmptyInput(sctx, input);
    ctx->SetOutputValue(kOut, result);
    return;
  }

  auto outputs = spu::kernel::hlo::GroupByAgg(sctx, {group_id}, {input},
                                              GetAggregationFunction(),
                                              {} /* useless valid bits */);
  YACL_ENFORCE(outputs.size() == 2, "GroupByAgg output size must be 2");

  // slice to get the result
  spu::NdArrayRef num_arr = spu::kernel::hal::dump_public(sctx, group_num);
  int64_t num = num_arr.at<uint32_t>(0);

  auto result = spu::kernel::hlo::Slice(sctx, outputs[1], {0}, {num}, {});
  ctx->SetOutputValue(kOut, result);
}

// GroupSecretSum implementation
const std::string GroupSecretSum::kOpType("GroupSecretSum");

spu::kernel::hlo::AggFunc GroupSecretSum::GetAggregationFunction() const {
  return spu::kernel::hlo::AggFunc::Sum;
}

// GroupSecretAvg implementation
const std::string GroupSecretAvg::kOpType("GroupSecretAvg");

spu::kernel::hlo::AggFunc GroupSecretAvg::GetAggregationFunction() const {
  return spu::kernel::hlo::AggFunc::Avg;
}

spu::Value GroupSecretAvg::HandleEmptyInput(spu::SPUContext* sctx,
                                            const spu::Value& in) {
  return in.clone().setDtype(spu::DT_F64, true);
}

}  // namespace scql::engine::op
