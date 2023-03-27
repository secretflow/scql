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

#include "engine/operator/make_private.h"

#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string MakePrivate::kOpType = "MakePrivate";

const std::string& MakePrivate::Type() const { return kOpType; }

void MakePrivate::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  const auto& outputs = ctx->GetOutput(kOut);

  YACL_ENFORCE(inputs.size() == outputs.size(),
               "MakePrivate input {} and output {} should have the same size",
               kIn, kOut);
  // TODO(shunde.csd): support TENSORSTATUS_CIPHER inputs
  YACL_ENFORCE(util::AreTensorsStatusMatched(inputs, pb::TENSORSTATUS_SECRET),
               "MakePrivate input tensor's status should all be secret");
  YACL_ENFORCE(util::AreTensorsStatusMatched(outputs, pb::TENSORSTATUS_PRIVATE),
               "MakePrivate output tensor's status should all be private");

  std::vector<std::string> reveal_to_parties =
      ctx->GetStringValuesFromAttribute(kRevealToAttr);
  YACL_ENFORCE(reveal_to_parties.size() == 1,
               "MakePrivate operator attribute {} should have exactly 1 "
               "element, but got {}",
               kRevealToAttr, reveal_to_parties.size());
}

void MakePrivate::Execute(ExecContext* ctx) {
  auto reveal_to_party = ctx->GetStringValueFromAttribute(kRevealToAttr);
  auto reveal_to_rank = ctx->GetSession()->GetPartyRank(reveal_to_party);
  YACL_ENFORCE(reveal_to_rank != -1, "unknown rank for party {}",
               reveal_to_party);

  bool reveal_to_me = ctx->GetSession()->SelfPartyCode() == reveal_to_party;

  const auto& input_pbs = ctx->GetInput(kIn);
  const auto& output_pbs = ctx->GetOutput(kOut);
  util::SpuOutfeedHelper io(ctx->GetSession()->GetSpuHalContext(),
                            ctx->GetSession()->GetDeviceSymbols());
  for (int i = 0; i < input_pbs.size(); ++i) {
    auto t = io.RevealTo(input_pbs[i].name(), reveal_to_rank);
    if (reveal_to_me) {
      if (output_pbs[i].elem_type() == pb::PrimitiveDataType::STRING) {
        t = ctx->GetSession()->HashToString(*t);
      }
      ctx->GetTensorTable()->AddTensor(output_pbs[i].name(), std::move(t));
    }
  }
}

}  // namespace scql::engine::op