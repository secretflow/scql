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

#include "engine/operator/shuffle.h"

#include "libspu/kernel/hlo/shuffle.h"

#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string Shuffle::kOpType("Shuffle");
const std::string& Shuffle::Type() const { return kOpType; }

void Shuffle::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  const auto& outputs = ctx->GetOutput(kOut);

  YACL_ENFORCE(inputs.size() > 0,
               "operator Shuffle input {} must have one element at least", kIn);
  YACL_ENFORCE(inputs.size() == outputs.size(),
               "operator Shuffle output and input should have the same size");

  YACL_ENFORCE(util::AreTensorsStatusMatched(inputs, pb::TENSORSTATUS_SECRET));
  YACL_ENFORCE(util::AreTensorsStatusMatched(outputs, pb::TENSORSTATUS_SECRET));
}

void Shuffle::Execute(ExecContext* ctx) {
  auto* symbols = ctx->GetSession()->GetDeviceSymbols();
  std::vector<spu::Value> inputs;

  const auto& input_pbs = ctx->GetInput(kIn);
  for (const auto& input_pb : input_pbs) {
    auto val =
        symbols->getVar(util::SpuVarNameEncoder::GetValueName(input_pb.name()));
    inputs.push_back(std::move(val));
  }

  auto* sctx = ctx->GetSession()->GetSpuContext();
  auto outputs = spu::kernel::hlo::Shuffle(sctx, inputs, 0);

  const auto& output_pbs = ctx->GetOutput(kOut);
  for (int i = 0; i < output_pbs.size(); ++i) {
    const auto& val_name =
        util::SpuVarNameEncoder::GetValueName(output_pbs[i].name());
    symbols->setVar(val_name, outputs[i]);
  }
}

}  // namespace scql::engine::op