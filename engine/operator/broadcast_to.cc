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

#include "engine/operator/broadcast_to.h"

#include "libspu/device/symbol_table.h"
#include "libspu/kernel/hal/shape_ops.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor_constructor.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string BroadcastTo::kOpType("BroadcastTo");

const std::string& BroadcastTo::Type() const { return kOpType; }

void BroadcastTo::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  const auto& ref_tensors = ctx->GetInput(kShapeRefTensor);
  const auto& outputs = ctx->GetOutput(kOut);

  YACL_ENFORCE(inputs.size() > 0, "BroadcastTo input size must > 0");
  YACL_ENFORCE(ref_tensors.size() == 1,
               "BroadcastTo ref_tensor size must == 1");
  YACL_ENFORCE(inputs.size() == outputs.size(),
               "BroadcastTo input {} and output {} should have the same size",
               kIn, kOut);

  YACL_ENFORCE(util::AreTensorsStatusMatched(inputs, pb::TENSORSTATUS_PUBLIC),
               "BroadcastTo inputs' statuses must be public");
  auto ref_tensor_status = util::GetTensorStatus(ref_tensors[0]);
  if (ref_tensor_status == pb::TENSORSTATUS_PRIVATE) {
    YACL_ENFORCE(
        util::AreTensorsStatusMatched(outputs, pb::TENSORSTATUS_PRIVATE),
        "BroadcastTo outputs' statuses must be private when ref_tensor is "
        "private");
  } else {
    YACL_ENFORCE(
        util::AreTensorsStatusMatched(outputs, pb::TENSORSTATUS_PUBLIC),
        "BroadcastTo outputs' statuses must be public when ref_tensor is "
        "not private");
  }
}

void BroadcastTo::Execute(ExecContext* ctx) {
  const auto& input_pbs = ctx->GetInput(kIn);
  const auto& output_pbs = ctx->GetOutput(kOut);
  const auto& ref_pb = ctx->GetInput(kShapeRefTensor)[0];
  if (util::GetTensorStatus(ref_pb) == pb::TENSORSTATUS_PRIVATE) {
    auto ref_tensor = ctx->GetTensorTable()->GetTensor(ref_pb.name());
    YACL_ENFORCE(ref_tensor, "get private shape ref tensor {} failed",
                 ref_pb.name());

    return BroadcastToPrivate(ctx, input_pbs, output_pbs, ref_tensor->Length());
  } else {
    const auto ref_tensor_name =
        util::SpuVarNameEncoder::GetValueName(ref_pb.name());
    auto symbols = ctx->GetSession()->GetDeviceSymbols();
    auto ref_tensor = symbols->getVar(ref_tensor_name);
    auto to_length = ref_tensor.shape().size() > 0 ? ref_tensor.shape()[0]
                                                   : ref_tensor.numel();
    return BroadcastToPublic(ctx, input_pbs, output_pbs, to_length);
  }
}

void BroadcastTo::BroadcastToPrivate(ExecContext* ctx,
                                     const RepeatedPbTensor& input_pbs,
                                     const RepeatedPbTensor& output_pbs,
                                     const int64_t to_length) {
  auto spu_io = util::SpuOutfeedHelper(ctx->GetSession()->GetSpuContext(),
                                       ctx->GetSession()->GetDeviceSymbols());
  for (int i = 0; i < input_pbs.size(); ++i) {
    auto ret = spu_io.DumpPublic(input_pbs[i].name());
    YACL_ENFORCE(ret, "dump public for {} failed", input_pbs[i].name());
    // convert hash to string for string tensor in spu
    if (input_pbs[i].elem_type() == pb::PrimitiveDataType::STRING) {
      ret = ctx->GetSession()->HashToString(*ret);
    }

    std::shared_ptr<arrow::Scalar> scalar;
    ASSIGN_OR_THROW_ARROW_STATUS(scalar,
                                 ret->ToArrowChunkedArray()->GetScalar(0));

    std::shared_ptr<arrow::Array> array;
    ASSIGN_OR_THROW_ARROW_STATUS(
        array, arrow::MakeArrayFromScalar(*scalar, to_length));

    auto chunked_arr = std::make_shared<arrow::ChunkedArray>(array);
    ctx->GetTensorTable()->AddTensor(output_pbs[i].name(),
                                     TensorFrom(chunked_arr));
  }
}

void BroadcastTo::BroadcastToPublic(ExecContext* ctx,
                                    const RepeatedPbTensor& input_pbs,
                                    const RepeatedPbTensor& output_pbs,
                                    const int64_t to_length) {
  auto* symbols = ctx->GetSession()->GetDeviceSymbols();
  auto* sctx = ctx->GetSession()->GetSpuContext();
  for (int i = 0; i < input_pbs.size(); ++i) {
    const auto value_name =
        util::SpuVarNameEncoder::GetValueName(input_pbs[i].name());
    auto value = symbols->getVar(value_name);

    auto result = spu::kernel::hal::broadcast_to(sctx, value, {to_length});

    symbols->setVar(util::SpuVarNameEncoder::GetValueName(output_pbs[i].name()),
                    result);
  }
}

}  // namespace scql::engine::op