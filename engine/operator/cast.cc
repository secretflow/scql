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

#include "engine/operator/cast.h"

#include "arrow/compute/cast.h"
#include "libspu/core/encoding.h"
#include "libspu/kernel/hlo/casting.h"

#include "engine/core/tensor_constructor.h"
#include "engine/core/type.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string Cast::kOpType("Cast");

const std::string& Cast::Type() const { return kOpType; }

void Cast::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  const auto& outputs = ctx->GetOutput(kOut);

  YACL_ENFORCE(inputs.size() == 1, "Cast input size={} not equal to 1",
               inputs.size());
  YACL_ENFORCE(outputs.size() == 1, "Cast output size={} not equal to 1",
               outputs.size());

  YACL_ENFORCE(
      util::IsTensorStatusMatched(outputs[0], util::GetTensorStatus(inputs[0])),
      "Cast output tensor's status should  be the same with input");
}

void Cast::Execute(ExecContext* ctx) {
  const auto& output_pb = ctx->GetOutput(kOut)[0];
  if (ctx->GetInputStatus(kIn) == pb::TensorStatus::TENSORSTATUS_PRIVATE) {
    auto tensor = ctx->GetInputTensor(kIn);
    YACL_ENFORCE(tensor != nullptr, "get tensor from tensor table failed");

    auto in_type = tensor->ArrowType();
    auto to_type = ToArrowDataType(output_pb.elem_type());
    YACL_ENFORCE(to_type, "no arrow type for tensor type={}",
                 pb::PrimitiveDataType_Name(output_pb.elem_type()));

    arrow::compute::CastOptions options;
    options.allow_float_truncate = true;

    arrow::Result<arrow::Datum> result;

    if ((in_type->id() == arrow::Type::STRING ||
         in_type->id() == arrow::Type::LARGE_STRING) &&
        output_pb.elem_type() == pb::PrimitiveDataType::DATETIME) {
      auto intermediate_type = arrow::timestamp(arrow::TimeUnit::SECOND);
      auto intermediate_result = arrow::compute::Cast(
          tensor->ToArrowChunkedArray(), intermediate_type);
      YACL_ENFORCE(intermediate_result.ok(),
                   "caught error while invoking arrow cast: {}",
                   intermediate_result.status().ToString());

      result = arrow::compute::Cast(
          intermediate_result.ValueOrDie().chunked_array(), to_type, options);
    } else {
      result =
          arrow::compute::Cast(tensor->ToArrowChunkedArray(), to_type, options);
    }

    YACL_ENFORCE(result.ok(), "caught error while invoking arrow cast: {}",
                 result.status().ToString());

    auto t = TensorFrom(result.ValueOrDie().chunked_array());
    ctx->SetOutputTensor(kOut, std::move(t));
    return;
  }

  YACL_ENFORCE(
      ctx->GetInput(kIn)[0].elem_type() != pb::PrimitiveDataType::STRING &&
          output_pb.elem_type() != pb::PrimitiveDataType::STRING,
      "string in spu is hash, not support cast");
  auto* sctx = ctx->GetSession()->GetSpuContext();
  auto to_type = spu::getEncodeType(DataTypeToSpuPtType(output_pb.elem_type()));

  auto value = ctx->GetInputValue(kIn);

  auto result_value =
      spu::kernel::hlo::Cast(sctx, value, value.vtype(), to_type);

  ctx->SetOutputValue(kOut, result_value);

#ifdef SCQL_WITH_NULL
  auto validity = ctx->GetInputValidity(kIn);

  ctx->SetOutputValidity(kOut, validity);
#endif  // SCQL_WITH_NULL
}

}  // namespace scql::engine::op
