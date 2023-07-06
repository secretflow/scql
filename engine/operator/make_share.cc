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

#include "engine/operator/make_share.h"

#include "libspu/device/io.h"

#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string MakeShare::kOpType("MakeShare");

const std::string& MakeShare::Type() const { return kOpType; }

void MakeShare::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  const auto& outputs = ctx->GetOutput(kOut);

  YACL_ENFORCE(inputs.size() == outputs.size(),
               "MakeShare input {} and output {} should have the same size",
               kIn, kOut);
  YACL_ENFORCE(util::AreTensorsStatusMatched(
                   inputs, pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "MakeShare input tensors' status should all be private");
  YACL_ENFORCE(util::AreTensorsStatusMatched(
                   outputs, pb::TensorStatus::TENSORSTATUS_SECRET),
               "MakeShare output tensors' status should all be secret");
}

void MakeShare::Execute(ExecContext* ctx) {
  spu::device::ColocatedIo cio(ctx->GetSession()->GetSpuContext());
  util::SpuInfeedHelper infeed_helper(&cio);

  const auto& input_pbs = ctx->GetInput(kIn);
  const auto& output_pbs = ctx->GetOutput(kOut);
  for (int i = 0; i < input_pbs.size(); ++i) {
    const auto& input_pb = input_pbs[i];
    const auto& output_pb = output_pbs[i];

    auto in_t = ctx->GetTensorTable()->GetTensor(input_pb.name());
    if (in_t == nullptr) {
      // does not own the tensor, skip it
      continue;
    }
    // NOTE: if tensor' type is string, we should convert it to
    // integer first, currently use hash value of string.
    if (in_t->Type() == pb::PrimitiveDataType::STRING) {
      in_t = ctx->GetSession()->StringToHash(*in_t);
    }
    infeed_helper.InfeedTensorAsSecret(output_pb.name(), *in_t);
  }

  infeed_helper.Sync();

  // merge symbols
  auto& symbols = cio.deviceSymbols();

  ctx->GetSession()->MergeDeviceSymbolsFrom(symbols);
}

}  // namespace scql::engine::op