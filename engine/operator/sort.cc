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

#include "engine/operator/sort.h"

#include "libspu/kernel/hlo/basic_binary.h"
#include "libspu/kernel/hlo/const.h"
#include "libspu/kernel/hlo/sort.h"

#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string Sort::kOpType("Sort");
const std::string& Sort::Type() const { return kOpType; }

void Sort::Validate(ExecContext* ctx) {
  const auto& sort_keys = ctx->GetInput(kInKey);
  const auto& inputs = ctx->GetInput(kIn);
  const auto& outputs = ctx->GetOutput(kOut);
  YACL_ENFORCE(sort_keys.size() > 0);
  YACL_ENFORCE(inputs.size() > 0);
  YACL_ENFORCE(inputs.size() == outputs.size());

  auto input_status = util::GetTensorStatus(inputs[0]);
  YACL_ENFORCE(input_status == pb::TENSORSTATUS_PRIVATE ||
                   input_status == pb::TENSORSTATUS_SECRET,
               "operator sort only supports private or secret inputs");
  // input and output should have the same status
  YACL_ENFORCE(util::AreTensorsStatusMatched(sort_keys, input_status));
  YACL_ENFORCE(util::AreTensorsStatusMatched(inputs, input_status));
  YACL_ENFORCE(util::AreTensorsStatusMatched(outputs, input_status));
}

void Sort::Execute(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  auto input_status = util::GetTensorStatus(inputs[0]);
  if (input_status == pb::TENSORSTATUS_PRIVATE) {
    return SortInPlain(ctx);
  } else {
    return SortInSecret(ctx);
  }
}

void Sort::SortInPlain(ExecContext* ctx) {
  YACL_THROW("SortInPlain is unimplemented");
}

void Sort::SortInSecret(ExecContext* ctx) {
  const auto& sort_key_pbs = ctx->GetInput(kInKey);
  const auto& in_pbs = ctx->GetInput(kIn);
  const auto& out_pbs = ctx->GetOutput(kOut);

  bool reverse = ctx->GetBooleanValueFromAttribute(kReverseAttr);

  auto symbols = ctx->GetSession()->GetDeviceSymbols();
  std::vector<spu::Value> inputs;
  for (const auto& sort_key_pb : sort_key_pbs) {
    auto value = symbols->getVar(
        util::SpuVarNameEncoder::GetValueName(sort_key_pb.name()));
    inputs.push_back(value);
  }

  size_t sort_key_num = inputs.size();

  for (const auto& in_pb : in_pbs) {
    auto value =
        symbols->getVar(util::SpuVarNameEncoder::GetValueName(in_pb.name()));
    inputs.push_back(value);
  }

  auto sctx = ctx->GetSession()->GetSpuContext();
  auto sort_direction = spu::kernel::hal::SortDirection::Ascending;
  if (reverse) {
    sort_direction = spu::kernel::hal::SortDirection::Descending;
  }
  // NOTE: use default performance hint for SimpleSort
  auto results = spu::kernel::hlo::SimpleSort(sctx, inputs, 0, sort_direction,
                                              sort_key_num);

  for (int i = 0; i < out_pbs.size(); ++i) {
    auto idx = sort_key_num + i;
    symbols->setVar(util::SpuVarNameEncoder::GetValueName(out_pbs[i].name()),
                    results[idx]);
  }

  // TODO: sort validity too
}

}  // namespace scql::engine::op