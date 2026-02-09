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

#include "engine/operator/logical.h"

#include "arrow/compute/api.h"
#include "libspu/kernel/hlo/basic_binary.h"
#include "libspu/kernel/hlo/basic_unary.h"

#include "engine/core/tensor_constructor.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string Not::kOpType("Not");
const std::string& Not::Type() const { return kOpType; }

void Not::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  const auto& outputs = ctx->GetOutput(kOut);

  YACL_ENFORCE(
      inputs.size() == outputs.size(),
      "Not operator inputs {} and outputs {} should have the same size", kIn,
      kOut);

  YACL_ENFORCE(
      util::AreTensorsStatusEqualAndOneOf(
          inputs, std::vector<pb::TensorStatus>{pb::TENSORSTATUS_PRIVATE,
                                                pb::TENSORSTATUS_SECRET}),
      "Not operator inputs {} status should be same and in [private, secret]",
      kIn);
  auto input_status = util::GetTensorStatus(inputs[0]);
  YACL_ENFORCE(
      util::AreTensorsStatusMatched(outputs, input_status),
      "Not operator inputs {} and outputs {} should have the same status", kIn,
      kOut);

  // Check input & output data type
  const auto input_type = inputs[0].elem_type();
  const auto output_type = outputs[0].elem_type();
  YACL_ENFORCE(input_type == pb::PrimitiveDataType::BOOL &&
                   output_type == pb::PrimitiveDataType::BOOL,
               "Not operator both input and output data type should be BOOL, "
               "but got input={}, output={}",
               pb::PrimitiveDataType_Name(input_type),
               pb::PrimitiveDataType_Name(output_type));
}

void Not::Execute(ExecContext* ctx) {
  if (ctx->GetInputStatus(kIn) == pb::TENSORSTATUS_PRIVATE) {
    return ExecuteInPlain(ctx);
  } else {
    return ExecuteInSecret(ctx);
  }
}

void Not::ExecuteInPlain(ExecContext* ctx) {
  auto input_tensors = ctx->GetInputTensors(kIn);
  std::vector<std::shared_ptr<Tensor>> results;
  results.reserve(input_tensors.size());

  for (const auto& input_tensor : input_tensors) {
    auto result = arrow::compute::CallFunction(
        "invert", {input_tensor->ToArrowChunkedArray()});
    YACL_ENFORCE(result.ok(),
                 "caught error while invoking arrow invert function: {}",
                 result.status().ToString());
    results.push_back(TensorFrom(result.ValueOrDie().chunked_array()));
  }

  ctx->SetOutputTensors(kOut, results);
}

void Not::ExecuteInSecret(ExecContext* ctx) {
  auto* sctx = ctx->GetSession()->GetSpuContext();
  auto input_values = ctx->GetInputValues(kIn);
  std::vector<spu::Value> results;
  results.reserve(input_values.size());

  for (const auto& input_value : input_values) {
    results.push_back(spu::kernel::hlo::Not(sctx, input_value));
  }

  ctx->SetOutputValues(kOut, results);

#ifdef SCQL_WITH_NULL
  auto input_validities = ctx->GetInputValidities(kIn);
  ctx->SetOutputValidities(kOut, input_validities);
#endif  // SCQL_WITH_NULL
}

void LogicalBase::ValidateIoDataTypes(ExecContext* ctx) {
  const auto input_left_type = ctx->GetInput(kInLeft)[0].elem_type();
  const auto input_right_type = ctx->GetInput(kInRight)[0].elem_type();
  const auto output_type = ctx->GetOutput(kOut)[0].elem_type();
  YACL_ENFORCE(
      input_left_type == pb::PrimitiveDataType::BOOL &&
          input_right_type == pb::PrimitiveDataType::BOOL,
      "Operator {} input data type should be BOOL, but got left={}, right={}",
      Type(), pb::PrimitiveDataType_Name(input_left_type),
      pb::PrimitiveDataType_Name(input_right_type));
  YACL_ENFORCE(output_type == pb::PrimitiveDataType::BOOL,
               "Operator {} output data type should be BOOL, but got={}",
               Type(), pb::PrimitiveDataType_Name(output_type));
}

// ===========================
//   LogicalAnd impl
// ===========================

const std::string LogicalAnd::kOpType("LogicalAnd");
const std::string& LogicalAnd::Type() const { return kOpType; }

spu::Value LogicalAnd::ComputeOnSpu(spu::SPUContext* sctx,
                                    const spu::Value& lhs,
                                    const spu::Value& rhs) {
  return spu::kernel::hlo::And(sctx, lhs, rhs);
}

TensorPtr LogicalAnd::ComputeInPlain(const Tensor& lhs, const Tensor& rhs) {
  auto result = arrow::compute::CallFunction(
      "and_kleene", {lhs.ToArrowChunkedArray(), rhs.ToArrowChunkedArray()});
  YACL_ENFORCE(result.ok(),
               "caught error while invoking arrow and function: {}",
               result.status().ToString());
  return TensorFrom(result.ValueOrDie().chunked_array());
}

// ===========================
//   LogicalOr impl
// ===========================

const std::string LogicalOr::kOpType("LogicalOr");
const std::string& LogicalOr::Type() const { return kOpType; }

spu::Value LogicalOr::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& lhs,
                                   const spu::Value& rhs) {
  return spu::kernel::hlo::Or(sctx, lhs, rhs);
}

TensorPtr LogicalOr::ComputeInPlain(const Tensor& lhs, const Tensor& rhs) {
  auto result = arrow::compute::CallFunction(
      "or_kleene", {lhs.ToArrowChunkedArray(), rhs.ToArrowChunkedArray()});
  YACL_ENFORCE(result.ok(), "caught error while invoking arrow or function: {}",
               result.status().ToString());
  return TensorFrom(result.ValueOrDie().chunked_array());
}

}  // namespace scql::engine::op
