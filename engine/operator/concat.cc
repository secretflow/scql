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

#include "engine/operator/concat.h"

#include "libspu/core/encoding.h"
#include "libspu/kernel/hlo/casting.h"
#include "libspu/kernel/hlo/geometrical.h"

#include "engine/core/primitive_builder.h"
#include "engine/core/type.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string Concat::kOpType("Concat");

const std::string& Concat::Type() const { return kOpType; }

void Concat::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  const auto& outputs = ctx->GetOutput(kOut);

  YACL_ENFORCE(inputs.size() > 0, "Concat input size must > 0");
  YACL_ENFORCE(outputs.size() == 1, "Concatoutput size={} not equal to 1",
               outputs.size());

  YACL_ENFORCE(util::AreTensorsStatusMatched(
                   inputs, pb::TensorStatus::TENSORSTATUS_SECRET),
               "Concat input tensors' statuses should all be secret");
  YACL_ENFORCE(util::IsTensorStatusMatched(
                   outputs[0], pb::TensorStatus::TENSORSTATUS_SECRET),
               "Concat output tensor's status should  be secret");
}

void Concat::Execute(ExecContext* ctx) {
  const auto& input_pbs = ctx->GetInput(kIn);
  const auto& output_pb = ctx->GetOutput(kOut)[0];
  auto sctx = ctx->GetSession()->GetSpuContext();
  auto symbols = ctx->GetSession()->GetDeviceSymbols();

  spu::DataType output_type = spu::DataType::DT_INVALID;
  if (output_pb.elem_type() != pb::PrimitiveDataType::STRING) {
    output_type =
        spu::getEncodeType(DataTypeToSpuPtType(output_pb.elem_type()));
  }

  std::vector<spu::Value> values;
  values.reserve(input_pbs.size());
  for (int i = 0; i < input_pbs.size(); ++i) {
    auto value = symbols->getVar(
        util::SpuVarNameEncoder::GetValueName(input_pbs[i].name()));
    if (output_type != spu::DataType::DT_INVALID &&
        output_type != value.dtype()) {
      const auto cast_value =
          spu::kernel::hlo::Cast(sctx, value, value.vtype(), output_type);
      values.push_back(cast_value);
    } else {
      values.push_back(value);
    }
  }

  int64_t axis = ctx->GetInt64ValueFromAttribute(kAxis);
  auto result_value = spu::kernel::hlo::Concatenate(sctx, values, axis);

  symbols->setVar(util::SpuVarNameEncoder::GetValueName(output_pb.name()),
                  result_value);

#ifdef SCQL_WITH_NULL
  std::vector<spu::Value> validities;
  validities.reserve(input_pbs.size());
  for (int i = 0; i < input_pbs.size(); ++i) {
    auto validity = symbols->getVar(
        util::SpuVarNameEncoder::GetValidityName(input_pbs[i].name()));
    validities.push_back(validity);
  }

  auto result_validity = spu::kernel::hlo::Concatenate(sctx, validities, axis);

  symbols->setVar(util::SpuVarNameEncoder::GetValidityName(output_pb.name()),
                  result_validity);
#endif  // SCQL_WITH_NULL
}

}  // namespace scql::engine::op