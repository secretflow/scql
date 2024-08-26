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

#include "engine/operator/binary_base.h"

#include "libspu/kernel/hlo/basic_binary.h"

#include "engine/util/context_util.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

void BinaryBase::Validate(ExecContext* ctx) {
  const auto& left = ctx->GetInput(kInLeft);
  const auto& right = ctx->GetInput(kInRight);
  const auto& out = ctx->GetOutput(kOut);
  YACL_ENFORCE(left.size() == right.size(),
               "op {} input left and right must have the same size", Type());
  YACL_ENFORCE(out.size() == left.size(),
               "op {} output must have the same size as input", Type());

  auto left_input_status = util::GetTensorStatus(left[0]);
  YACL_ENFORCE(util::AreTensorsStatusMatched(left, left_input_status),
               "op {} input left should have the same status", Type());

  switch (left_input_status) {
    case pb::TENSORSTATUS_PRIVATE: {
      YACL_ENFORCE(util::AreTensorsStatusEqualAndOneOf(
          right, {pb::TENSORSTATUS_PRIVATE, pb::TENSORSTATUS_PUBLIC}));
      YACL_ENFORCE(
          util::AreTensorsStatusMatched(out, pb::TENSORSTATUS_PRIVATE));
      break;
    }
    case pb::TENSORSTATUS_SECRET: {
      YACL_ENFORCE(util::AreTensorsStatusEqualAndOneOf(
          right, {pb::TENSORSTATUS_SECRET, pb::TENSORSTATUS_PUBLIC}));
      YACL_ENFORCE(util::AreTensorsStatusMatched(out, pb::TENSORSTATUS_SECRET));
      break;
    }
    case pb::TENSORSTATUS_PUBLIC: {
      YACL_ENFORCE(util::AreTensorsStatusEqualAndOneOf(
          right, {pb::TENSORSTATUS_SECRET, pb::TENSORSTATUS_PRIVATE}));
      if (util::IsTensorStatusMatched(right[0], pb::TENSORSTATUS_SECRET)) {
        YACL_ENFORCE(
            util::AreTensorsStatusMatched(out, pb::TENSORSTATUS_SECRET));
      } else {
        YACL_ENFORCE(
            util::AreTensorsStatusMatched(out, pb::TENSORSTATUS_PRIVATE));
      }
      break;
    }
    default:
      YACL_THROW("unsupported input status: {}",
                 pb::TensorStatus_Name(util::GetTensorStatus(left[0])));
  }
  ValidateIoDataTypes(ctx);
}

void BinaryBase::Execute(ExecContext* ctx) {
  const auto out_status = util::GetTensorStatus(ctx->GetOutput(kOut)[0]);

  if (out_status == pb::TENSORSTATUS_PRIVATE) {
    return ExecuteInPlain(ctx);
  } else {
    return ExecuteInSecret(ctx);
  }
}

void BinaryBase::ExecuteInSecret(ExecContext* ctx) {
  const auto& left_pbs = ctx->GetInput(kInLeft);
  const auto& right_pbs = ctx->GetInput(kInRight);
  const auto& out_pbs = ctx->GetOutput(kOut);

  auto device_symbols = ctx->GetSession()->GetDeviceSymbols();
  auto sctx = ctx->GetSession()->GetSpuContext();

  // TODO(shunde.csd): possible optimization
  // maybe we should concatenate values together to reduce communication
  // round-trip
  for (int i = 0; i < left_pbs.size(); ++i) {
    const auto& left_param = left_pbs[i];
    const auto& right_param = right_pbs[i];
    const auto& out_param = out_pbs[i];

    auto left_value = device_symbols->getVar(
        util::SpuVarNameEncoder::GetValueName(left_param.name()));
    auto right_value = device_symbols->getVar(
        util::SpuVarNameEncoder::GetValueName(right_param.name()));
    auto result_value = ComputeOnSpu(sctx, left_value, right_value);
    device_symbols->setVar(
        util::SpuVarNameEncoder::GetValueName(out_param.name()), result_value);

#ifdef SCQL_WITH_NULL
    auto left_validity = device_symbols->getVar(
        SpuVarNameEncoder::GetValidityName(left_param.name()));
    auto right_validity = device_symbols->getVar(
        SpuVarNameEncoder::GetValidityName(right_param.name()));
    auto result_validity = PropagateNulls(left_validity, right_validity);
    device_symbols->setVar(SpuVarNameEncoder::GetValidityName(out_param.name(), result_validity);
#endif  // SCQL_WITH_NULL
  }
}

void BinaryBase::ExecuteInPlain(ExecContext* ctx) {
  const auto& left_pbs = ctx->GetInput(kInLeft);
  const auto& right_pbs = ctx->GetInput(kInRight);
  const auto& out_pbs = ctx->GetOutput(kOut);

  for (int i = 0; i < left_pbs.size(); ++i) {
    const auto& left_param = left_pbs[i];
    const auto& right_param = right_pbs[i];
    const auto& out_param = out_pbs[i];

    TensorPtr left = util::GetPrivateOrPublicTensor(ctx, left_param);
    TensorPtr right = util::GetPrivateOrPublicTensor(ctx, right_param);
    YACL_ENFORCE(left != nullptr);
    YACL_ENFORCE(right != nullptr);

    auto result = ComputeInPlain(*left, *right);
    ctx->GetTensorTable()->AddTensor(out_param.name(), std::move(result));
  }
}

spu::Value BinaryBase::PropagateNulls(spu::SPUContext* sctx,
                                      const spu::Value& left,
                                      const spu::Value& right) {
  return spu::kernel::hlo::And(sctx, left, right);
}

}  // namespace scql::engine::op