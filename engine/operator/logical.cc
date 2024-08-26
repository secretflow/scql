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
  const auto& input_pb = ctx->GetInput(kIn)[0];
  auto input_status = util::GetTensorStatus(input_pb);
  if (input_status == pb::TENSORSTATUS_PRIVATE) {
    return ExecuteInPlain(ctx);
  } else if (input_status == pb::TENSORSTATUS_SECRET) {
    return ExecuteInSecret(ctx);
  } else {
    YACL_THROW("unsupported input status: {}",
               pb::TensorStatus_Name(input_status));
  }
}

void Not::ExecuteInPlain(ExecContext* ctx) {
  const auto& input_pbs = ctx->GetInput(kIn);
  const auto& output_pbs = ctx->GetOutput(kOut);

  auto tensor_table = ctx->GetSession()->GetTensorTable();
  for (int i = 0; i < input_pbs.size(); ++i) {
    const auto input_pb = input_pbs[i];
    const auto output_pb = output_pbs[i];

    auto in_t = tensor_table->GetTensor(input_pb.name());
    YACL_ENFORCE(in_t != nullptr, "input {} not found in tensor table",
                 input_pb.name());
    auto result =
        arrow::compute::CallFunction("invert", {in_t->ToArrowChunkedArray()});
    YACL_ENFORCE(result.ok(),
                 "caught error while invoking arrow invert function: {}",
                 result.status().ToString());
    auto out_t = TensorFrom(result.ValueOrDie().chunked_array());
    tensor_table->AddTensor(output_pb.name(), std::move(out_t));
  }
}

void Not::ExecuteInSecret(ExecContext* ctx) {
  const auto& input_pbs = ctx->GetInput(kIn);
  const auto& output_pbs = ctx->GetOutput(kOut);

  auto sctx = ctx->GetSession()->GetSpuContext();
  auto symbols = ctx->GetSession()->GetDeviceSymbols();
  for (int i = 0; i < input_pbs.size(); ++i) {
    const auto input_pb = input_pbs[i];
    const auto output_pb = output_pbs[i];

    auto in_val =
        symbols->getVar(util::SpuVarNameEncoder::GetValueName(input_pb.name()));

    auto out_val = spu::kernel::hlo::Not(sctx, in_val);

    symbols->setVar(util::SpuVarNameEncoder::GetValueName(output_pb.name()),
                    out_val);
#ifdef SCQL_WITH_NULL
    auto out_validity = symbols->getVar(
        util::SpuVarNameEncoder::GetValidityName(input_pb.name()));
    symbols->setVar(util::SpuVarNameEncoder::GetValidityName(output_pb.name()),
                    out_validity);
#endif  // SCQL_WITH_NULL
  }
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