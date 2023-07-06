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

#include "engine/operator/oblivious_group_mark.h"

#include "libspu/kernel/hal/shape_ops.h"
#include "libspu/kernel/hlo/basic_binary.h"
#include "libspu/kernel/hlo/casting.h"
#include "libspu/kernel/hlo/const.h"
#include "libspu/kernel/hlo/geometrical.h"

#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string ObliviousGroupMark::kOpType("ObliviousGroupMark");
const std::string& ObliviousGroupMark::Type() const { return kOpType; }

void ObliviousGroupMark::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  YACL_ENFORCE(inputs.size() > 0, "input size cannot be 0");
  const auto& outputs = ctx->GetOutput(kOut);
  YACL_ENFORCE(outputs.size() == 1, "outputs' size={} not equal to 1",
               outputs.size());

  YACL_ENFORCE(util::AreTensorsStatusMatched(inputs, pb::TENSORSTATUS_SECRET),
               "inputs' status are not all secret");
  YACL_ENFORCE(util::AreTensorsStatusMatched(outputs, pb::TENSORSTATUS_SECRET),
               "outputs' status are not all secret");
}

void ObliviousGroupMark::Execute(ExecContext* ctx) {
  const auto& input_pbs = ctx->GetInput(kIn);

  auto symbols = ctx->GetSession()->GetDeviceSymbols();
  auto sctx = ctx->GetSession()->GetSpuContext();
  // TODO(jingshi) : check and calculate validity
  auto first_value = symbols->getVar(
      util::SpuVarNameEncoder::GetValueName(input_pbs[0].name()));
  int64_t row_count = first_value.shape().size() > 0 ? first_value.shape()[0]
                                                     : first_value.numel();
  spu::Value result;
  if (row_count <= 1) {
    result = spu::kernel::hlo::Seal(
        sctx, spu::kernel::hlo::Constant(sctx, true, {row_count}));
  } else {
    result = GetFullGroupMark(ctx);
  }

  const auto& output_pbs = ctx->GetOutput(kOut);
  symbols->setVar(util::SpuVarNameEncoder::GetValueName(output_pbs[0].name()),
                  result);
}

// get group for row count >= 2
// e.g: first_value = {0, 1, 1, 1, 2}
// from_top = {0, 1, 1, 1}, to_bottom = {1, 1, 1, 2}
// result = {1, 0, 0, 1}, full_result need to add tail = {1}
spu::Value ObliviousGroupMark::GetFullGroupMark(ExecContext* ctx) {
  const auto& input_pbs = ctx->GetInput(kIn);
  auto symbols = ctx->GetSession()->GetDeviceSymbols();
  auto sctx = ctx->GetSession()->GetSpuContext();
  auto first_value = symbols->getVar(
      util::SpuVarNameEncoder::GetValueName(input_pbs[0].name()));
  int64_t row_count = first_value.shape().size() > 0 ? first_value.shape()[0]
                                                     : first_value.numel();
  auto from_top =
      spu::kernel::hal::slice(sctx, first_value, {0}, {row_count - 1}, {});
  auto to_bottom =
      spu::kernel::hal::slice(sctx, first_value, {1}, {row_count}, {});

  auto result = spu::kernel::hlo::NotEqual(sctx, from_top, to_bottom);

  for (int i = 1; i < input_pbs.size(); ++i) {
    auto cur_value = symbols->getVar(
        util::SpuVarNameEncoder::GetValueName(input_pbs[i].name()));
    int64_t cur_row_count =
        cur_value.shape().size() > 0 ? cur_value.shape()[0] : cur_value.numel();
    YACL_ENFORCE(cur_row_count == row_count,
                 "intput tensor#{} row count not equal to the previous");

    auto from_top =
        spu::kernel::hal::slice(sctx, cur_value, {0}, {row_count - 1}, {});
    auto to_bottom =
        spu::kernel::hal::slice(sctx, cur_value, {1}, {row_count}, {});

    auto cur_result = spu::kernel::hlo::NotEqual(sctx, from_top, to_bottom);

    result = spu::kernel::hlo::Or(sctx, result, cur_result);
  }

  auto tail =
      spu::kernel::hlo::Seal(sctx, spu::kernel::hlo::Constant(sctx, true, {1}));

  auto full_result = spu::kernel::hlo::Pad(sctx, result, tail, {0}, {1}, {0});
  return full_result;
}

};  // namespace scql::engine::op