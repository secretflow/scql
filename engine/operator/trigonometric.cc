// Copyright 2024 Ant Group Co., Ltd.
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

#include "engine/operator/trigonometric.h"

#include "arrow/compute/exec.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "libspu/kernel/hlo/basic_binary.h"
#include "libspu/kernel/hlo/basic_unary.h"
#include "libspu/kernel/hlo/casting.h"

#include "engine/core/tensor.h"
#include "engine/core/tensor_constructor.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {
void TrigonometricFunction::Execute(ExecContext* ctx) {
  const auto output_status = util::GetTensorStatus(ctx->GetOutput(kOut)[0]);

  if (output_status == pb::TENSORSTATUS_PRIVATE) {
    ExecuteInPlain(ctx);
  } else {
    ExecuteInSecret(ctx);
  }
}

void TrigonometricFunction::Validate(ExecContext* ctx) {
  const auto& input = ctx->GetInput(kIn);
  const auto& output = ctx->GetOutput(kOut);

  YACL_ENFORCE(input.size() == 1, "Sine function input size={} not equal to 1",
               input.size());
  YACL_ENFORCE(output.size() == 1,
               "Sine function output size={} not equal to 1", output.size());

  YACL_ENFORCE(
      util::IsTensorStatusMatched(output[0], util::GetTensorStatus(input[0])),
      "Sine function output tensor's status should be same with input");
  YACL_ENFORCE(output[0].elem_type() == pb::PrimitiveDataType::FLOAT64,
               "the output type of trigonometric must be float64");
}

void TrigonometricFunction::ExecuteInSecret(ExecContext* ctx) {
  const auto& input_pb = ctx->GetInput(kIn);
  const auto& output_pb = ctx->GetOutput(kOut);

  auto* device_symbols = ctx->GetSession()->GetDeviceSymbols();
  auto* sctx = ctx->GetSession()->GetSpuContext();

  for (int i = 0; i < input_pb.size(); i++) {
    auto spu_value = device_symbols->getVar(
        util::SpuVarNameEncoder::GetValueName(input_pb[i].name()));

    if (!spu_value.isFxp()) {
      auto to_type = spu::DataType::DT_F64;
      spu_value =
          spu::kernel::hlo::Cast(sctx, spu_value, spu_value.vtype(), to_type);
    }

    auto result = ComputeOnSpu(sctx, spu_value);
    device_symbols->setVar(
        util::SpuVarNameEncoder::GetValueName(output_pb[i].name()), result);
  }
}

void TrigonometricFunction::ExecuteTrigonometricFunction(
    ExecContext* ctx, const std::string& func) {
  const auto& input_pbs = ctx->GetInput(kIn);
  const auto& output_pbs = ctx->GetOutput(kOut);

  for (int i = 0; i < input_pbs.size(); i++) {
    TensorPtr input_ptr = ctx->GetTensorTable()->GetTensor(input_pbs[i].name());
    YACL_ENFORCE(input_ptr != nullptr, "input({}) is null for {} function",
                 input_pbs[i].name(), func);
    auto result =
        arrow::compute::CallFunction(func, {input_ptr->ToArrowChunkedArray()});
    YACL_ENFORCE(result.ok(), "failed to run '{}' function", func);
    TensorPtr resultTensor = TensorFrom(result.ValueOrDie().chunked_array());
    ctx->GetTensorTable()->AddTensor(output_pbs[i].name(),
                                     std::move(resultTensor));
  }
}

// ===========================
//   Sin impl
// ===========================
const std::string Sine::kOpType("Sin");

const std::string& Sine::Type() const { return kOpType; }

void Sine::ExecuteInPlain(ExecContext* ctx) {
  ExecuteTrigonometricFunction(ctx, "sin");
}

spu::Value Sine::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& value) {
  return spu::kernel::hlo::Sine(sctx, value);
}

// ===========================
//   Cos impl
// ===========================
const std::string Cosine::kOpType("Cos");

const std::string& Cosine::Type() const { return kOpType; }

void Cosine::ExecuteInPlain(ExecContext* ctx) {
  ExecuteTrigonometricFunction(ctx, "cos");
}

spu::Value Cosine::ComputeOnSpu(spu::SPUContext* sctx,
                                const spu::Value& value) {
  return spu::kernel::hlo::Cosine(sctx, value);
}

// ===========================
//   Arc Cos impl
// ===========================
const std::string ACosine::kOpType("ACos");

const std::string& ACosine::Type() const { return kOpType; }

void ACosine::ExecuteInPlain(ExecContext* ctx) {
  ExecuteTrigonometricFunction(ctx, "acos");
}

spu::Value ACosine::ComputeOnSpu(spu::SPUContext* sctx,
                                 const spu::Value& value) {
  return spu::kernel::hlo::Acos(sctx, value);
}

// ===========================
//   Tan impl
// ===========================
const std::string Tan::kOpType("Tan");

const std::string& Tan::Type() const { return kOpType; }

void Tan::ExecuteInPlain(ExecContext* ctx) {
  ExecuteTrigonometricFunction(ctx, "tan");
}

spu::Value Tan::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& value) {
  YACL_THROW("secret tangent function is not implemented");
}

// ===========================
//   Arc sine impl
// ===========================
const std::string ASine::kOpType("ASin");
const std::string& ASine::Type() const { return kOpType; }
void ASine::ExecuteInPlain(ExecContext* ctx) {
  ExecuteTrigonometricFunction(ctx, "asin");
}
spu::Value ASine::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& value) {
  return spu::kernel::hlo::Asin(sctx, value);
}

// ===========================
//   Arc tan impl
// ===========================
const std::string ATan::kOpType("ATan");
const std::string& ATan::Type() const { return kOpType; }
void ATan::ExecuteInPlain(ExecContext* ctx) {
  ExecuteTrigonometricFunction(ctx, "atan");
}
spu::Value ATan::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& value) {
  YACL_THROW("secret atan function is not implemented");
}

// ===========================
//   ATan2 impl
// ===========================
const std::string ATan2::kOpType("ATan2");
const std::string& ATan2::Type() const { return kOpType; }

void ATan2::ValidateIoDataTypes(ExecContext* ctx) {
  const auto& output = ctx->GetOutput(kOut);
  YACL_ENFORCE(output.size() == 1, "output size of atan2 must be 1");
  YACL_ENFORCE(output[0].elem_type() == pb::PrimitiveDataType::FLOAT64 ||
                   output[0].elem_type() == pb::PrimitiveDataType::FLOAT32,
               "output data type must be float");
}

TensorPtr ATan2::ComputeInPlain(const Tensor& lhs, const Tensor& rhs) {
  arrow::Result<arrow::Datum> result = arrow::compute::CallFunction(
      "atan2", {lhs.ToArrowChunkedArray(), rhs.ToArrowChunkedArray()});
  YACL_ENFORCE(result.ok(), "failed to run atan2 function");
  return TensorFrom(result.ValueOrDie().chunked_array());
}

spu::Value ATan2::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& lhs,
                               const spu::Value& rhs) {
  return spu::kernel::hlo::Atan2(sctx, lhs, rhs);
}
}  // namespace scql::engine::op