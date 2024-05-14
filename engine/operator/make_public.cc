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

#include "engine/operator/make_public.h"

#include "libspu/device/symbol_table.h"
#include "libspu/kernel/hal/type_cast.h"

#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string MakePublic::kOpType("MakePublic");
const std::string& MakePublic::Type() const { return kOpType; }

void MakePublic::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  YACL_ENFORCE(inputs.size() > 0, "input size cannot be 0");
  const auto& outputs = ctx->GetOutput(kOut);
  YACL_ENFORCE(inputs.size() == outputs.size(),
               "inputs' size={} and outputs' size={} not equal", inputs.size(),
               outputs.size());

  YACL_ENFORCE(util::AreTensorsStatusEqualAndOneOf(
                   inputs, {pb::TENSORSTATUS_PRIVATE, pb::TENSORSTATUS_SECRET}),
               "inputs' status are not equal and one of private/secret");
  YACL_ENFORCE(util::AreTensorsStatusMatched(outputs, pb::TENSORSTATUS_PUBLIC),
               "outputs' status are not all public");
}

void MakePublic::Execute(ExecContext* ctx) {
  const auto& input_pbs = ctx->GetInput(kIn);
  const auto& output_pbs = ctx->GetOutput(kOut);
  const auto& input_status = util::GetTensorStatus(input_pbs[0]);
  if (input_status == pb::TENSORSTATUS_PRIVATE) {
    PrivateToPublic(ctx, input_pbs, output_pbs);
  } else if (input_status == pb::TENSORSTATUS_SECRET) {
    SecretToPublic(ctx, input_pbs, output_pbs);
  } else {
    YACL_THROW("input status={} not support in make_public",
               pb::TensorStatus_Name(input_status));
  }
}

void MakePublic::PrivateToPublic(ExecContext* ctx,
                                 const RepeatedPbTensor& inputs,
                                 const RepeatedPbTensor& outputs) {
  spu::device::ColocatedIo cio(ctx->GetSession()->GetSpuContext());
  util::SpuInfeedHelper infeed_helper(&cio);

  for (int i = 0; i < inputs.size(); ++i) {
    const auto& in_name = inputs[i].name();
    auto in_t = ctx->GetTensorTable()->GetTensor(in_name);

    if (in_t != nullptr) {
      // NOTE: if tensor' type is string, we should convert it to
      // integer first, currently use hash value of string.
      if (in_t->Type() == pb::PrimitiveDataType::STRING) {
        in_t = ctx->GetSession()->StringToHash(*in_t);
      }
      const auto& out_name = outputs[i].name();
      infeed_helper.InfeedTensorAsPublic(out_name, *in_t);
    }
  }
  infeed_helper.Sync();

  // merge symbols
  auto& symbols = cio.deviceSymbols();
  ctx->GetSession()->MergeDeviceSymbolsFrom(symbols);
}

void MakePublic::SecretToPublic(ExecContext* ctx,
                                const RepeatedPbTensor& inputs,
                                const RepeatedPbTensor& outputs) {
  auto symbols = ctx->GetSession()->GetDeviceSymbols();
  for (int i = 0; i < inputs.size(); ++i) {
    const auto& in_name = inputs[i].name();
    auto secret_in_value =
        symbols->getVar(util::SpuVarNameEncoder::GetValueName(in_name));

    auto sctx = ctx->GetSession()->GetSpuContext();
    auto public_out_value = spu::kernel::hal::reveal(sctx, secret_in_value);

    const auto& out_name = outputs[i].name();
    symbols->setVar(util::SpuVarNameEncoder::GetValueName(out_name),
                    public_out_value);

#ifdef SCQL_WITH_NULL
    auto secret_in_validity =
        symbols->getVar(util::SpuVarNameEncoder::GetValidityName(in_name));

    auto public_out_validity =
        spu::kernel::hal::reveal(sctx, secret_in_validity);

    symbols->setVar(util::SpuVarNameEncoder::GetValidityName(out_name),
                    public_out_validity);
#endif  // SCQL_WITH_NULL
  }
}

};  // namespace scql::engine::op