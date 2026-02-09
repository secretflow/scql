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
  if (ctx->GetOutputStatus(kOut) == pb::TENSORSTATUS_PRIVATE) {
    ExecuteInPlain(ctx);
  } else {
    ExecuteInSecret(ctx);
  }
}

void BinaryBase::ExecuteInSecret(ExecContext* ctx) {
  const auto& left_inputs = ctx->GetInputValues(kInLeft);
  const auto& right_inputs = ctx->GetInputValues(kInRight);

  // TODO(shunde.csd): possible optimization
  // maybe we should concatenate values together to reduce communication
  // round-trip
  for (int i = 0; i < left_inputs.size(); ++i) {
    auto result_value = ComputeOnSpu(ctx->GetSession()->GetSpuContext(),
                                     left_inputs[i], right_inputs[i]);
    ctx->SetOutputValue(kOut, result_value, i);

#ifdef SCQL_WITH_NULL
    auto left_validity = ctx->GetInputValidity(kInLeft, i);
    auto right_validity = ctx->GetInputValidity(kInRight, i);
    auto result_validity = PropagateNulls(left_validity, right_validity);
    ctx->SetOutputValidity(kOut, result_validity, i);
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

    TensorPtr left = ctx->GetPrivateOrPublicTensor(left_param);
    TensorPtr right = ctx->GetPrivateOrPublicTensor(right_param);
    YACL_ENFORCE(left != nullptr);
    YACL_ENFORCE(right != nullptr);

    auto result = ComputeInPlain(*left, *right);
    ctx->SetOutputTensor(kOut, std::move(result), i);
  }
}

spu::Value BinaryBase::PropagateNulls(spu::SPUContext* sctx,
                                      const spu::Value& lhs,
                                      const spu::Value& rhs) {
  return spu::kernel::hlo::And(sctx, lhs, rhs);
}

}  // namespace scql::engine::op
