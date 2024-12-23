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

#include "engine/operator/compare.h"

#include "arrow/compute/api.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "libspu/kernel/hlo/basic_binary.h"
#include "libspu/kernel/hlo/casting.h"

#include "engine/core/tensor_constructor.h"
#include "engine/util/context_util.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

void CompareBase::ValidateIoDataTypes(ExecContext* ctx) {
  // TODO(jingshi) : add input type checks, maybe we should override this in
  // operators.

  const auto& outputs = ctx->GetOutput(kOut);
  const auto& output_type = outputs[0].elem_type();
  YACL_ENFORCE(output_type == pb::PrimitiveDataType::BOOL,
               "outputs' data type={}, only {} supported",
               pb::PrimitiveDataType_Name(output_type),
               pb::PrimitiveDataType_Name(pb::PrimitiveDataType::BOOL));
}

// ===========================
//   Equal impl
// ===========================

const std::string Equal::kOpType("Equal");

const std::string& Equal::Type() const { return kOpType; }

spu::Value Equal::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& lhs,
                               const spu::Value& rhs) {
  return spu::kernel::hlo::Equal(sctx, lhs, rhs);
}

TensorPtr Equal::ComputeInPlain(const Tensor& lhs, const Tensor& rhs) {
  arrow::Result<arrow::Datum> result = arrow::compute::CallFunction(
      "equal", {lhs.ToArrowChunkedArray(), rhs.ToArrowChunkedArray()});

  YACL_ENFORCE(result.ok(),
               "caught error while invoking arrow equal function: {}",
               result.status().ToString());
  return TensorFrom(result.ValueOrDie().chunked_array());
}

// ===========================
//   NotEqual impl
// ===========================

const std::string NotEqual::kOpType("NotEqual");

const std::string& NotEqual::Type() const { return kOpType; }

spu::Value NotEqual::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& lhs,
                                  const spu::Value& rhs) {
  return spu::kernel::hlo::NotEqual(sctx, lhs, rhs);
}

TensorPtr NotEqual::ComputeInPlain(const Tensor& lhs, const Tensor& rhs) {
  arrow::Result<arrow::Datum> result = arrow::compute::CallFunction(
      "not_equal", {lhs.ToArrowChunkedArray(), rhs.ToArrowChunkedArray()});

  YACL_ENFORCE(result.ok(),
               "caught error while invoking arrow not_equal function: {}",
               result.status().ToString());
  return TensorFrom(result.ValueOrDie().chunked_array());
}

// ===========================
//   Less impl
// ===========================

const std::string Less::kOpType("Less");

const std::string& Less::Type() const { return kOpType; }

spu::Value Less::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& lhs,
                              const spu::Value& rhs) {
  return spu::kernel::hlo::Less(sctx, lhs, rhs);
}

TensorPtr Less::ComputeInPlain(const Tensor& lhs, const Tensor& rhs) {
  arrow::Result<arrow::Datum> result = arrow::compute::CallFunction(
      "less", {lhs.ToArrowChunkedArray(), rhs.ToArrowChunkedArray()});

  YACL_ENFORCE(result.ok(),
               "caught error while invoking arrow less function: {}",
               result.status().ToString());
  return TensorFrom(result.ValueOrDie().chunked_array());
}

// ===========================
//   LessEqual impl
// ===========================

const std::string LessEqual::kOpType("LessEqual");

const std::string& LessEqual::Type() const { return kOpType; }

spu::Value LessEqual::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& lhs,
                                   const spu::Value& rhs) {
  return spu::kernel::hlo::LessEqual(sctx, lhs, rhs);
}

TensorPtr LessEqual::ComputeInPlain(const Tensor& lhs, const Tensor& rhs) {
  arrow::Result<arrow::Datum> result = arrow::compute::CallFunction(
      "less_equal", {lhs.ToArrowChunkedArray(), rhs.ToArrowChunkedArray()});

  YACL_ENFORCE(result.ok(),
               "caught error while invoking arrow less_equal function: {}",
               result.status().ToString());
  return TensorFrom(result.ValueOrDie().chunked_array());
}

// ===========================
//   GreaterEqual impl
// ===========================

const std::string GreaterEqual::kOpType("GreaterEqual");

const std::string& GreaterEqual::Type() const { return kOpType; }

spu::Value GreaterEqual::ComputeOnSpu(spu::SPUContext* sctx,
                                      const spu::Value& lhs,
                                      const spu::Value& rhs) {
  return spu::kernel::hlo::GreaterEqual(sctx, lhs, rhs);
}

TensorPtr GreaterEqual::ComputeInPlain(const Tensor& lhs, const Tensor& rhs) {
  arrow::Result<arrow::Datum> result = arrow::compute::CallFunction(
      "greater_equal", {lhs.ToArrowChunkedArray(), rhs.ToArrowChunkedArray()});

  YACL_ENFORCE(result.ok(),
               "caught error while invoking arrow greater_equal function: {}",
               result.status().ToString());
  return TensorFrom(result.ValueOrDie().chunked_array());
}

// ===========================
//   Greater impl
// ===========================

const std::string Greater::kOpType("Greater");

const std::string& Greater::Type() const { return kOpType; }

spu::Value Greater::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& lhs,
                                 const spu::Value& rhs) {
  return spu::kernel::hlo::Greater(sctx, lhs, rhs);
}

TensorPtr Greater::ComputeInPlain(const Tensor& lhs, const Tensor& rhs) {
  arrow::Result<arrow::Datum> result = arrow::compute::CallFunction(
      "greater", {lhs.ToArrowChunkedArray(), rhs.ToArrowChunkedArray()});

  YACL_ENFORCE(result.ok(),
               "caught error while invoking arrow greater function: {}",
               result.status().ToString());
  return TensorFrom(result.ValueOrDie().chunked_array());
}

// ===========================
//   Variadic definition
// ===========================

void Variadic::Validate(ExecContext* ctx) {
  YACL_ENFORCE(ctx, "context is null");
  auto inputs = ctx->GetInput(kIn);
  YACL_ENFORCE(inputs.size() >= 2,
               "variadic compare requires at least 2 inputs");
  auto output = ctx->GetOutput(kOut);
  YACL_ENFORCE(output.size() == 1, "multi-input compare requires one output");
}

void Variadic::ExecuteInPlain(ExecContext* ctx) {
  auto inputs = ctx->GetInput(kIn);
  std::vector<TensorPtr> input_ptrs;
  for (auto& input : inputs) {
    auto input_ptr = ctx->GetTensorTable()->GetTensor(input.name());
    YACL_ENFORCE(input_ptr != nullptr, "input tensor {} not found",
                 input.name());
    input_ptrs.push_back(input_ptr);
  }

  auto output_ptr = ComputeInPlain(input_ptrs);
  auto output = ctx->GetOutput(kOut);
  ctx->GetTensorTable()->AddTensor(output[0].name(), std::move(output_ptr));
}

void Variadic::ExecuteOnSpu(ExecContext* ctx) {
  auto* device_symbols = ctx->GetSession()->GetDeviceSymbols();
  YACL_ENFORCE(device_symbols != nullptr, "device_symbols is nullptr");
  auto* sctx = ctx->GetSession()->GetSpuContext();
  YACL_ENFORCE(sctx != nullptr, "spu context is nullptr");
  const auto& in_pb = ctx->GetInput(kIn);
  std::vector<spu::Value> input_values;
  for (const auto& input : in_pb) {
    input_values.push_back(device_symbols->getVar(
        util::SpuVarNameEncoder::GetValueName(input.name())));
  }
  auto output_value = ComputeOnSpu(sctx, input_values);
  const auto& out_pb = ctx->GetOutput(kOut);
  device_symbols->setVar(
      util::SpuVarNameEncoder::GetValueName(out_pb[0].name()), output_value);
}

void Variadic::Execute(ExecContext* ctx) {
  auto inputs = ctx->GetInput(kIn);
  auto outputs = ctx->GetOutput(kOut);
  if (util::GetTensorStatus(outputs[0]) == pb::TENSORSTATUS_PRIVATE) {
    ExecuteInPlain(ctx);
  } else {
    ExecuteOnSpu(ctx);
  }
}

// ===========================
//   Greatest impl
// ===========================
const std::string Greatest::kOpType("Greatest");
const std::string& Greatest::Type() const { return kOpType; }

TensorPtr Greatest::ComputeInPlain(const std::vector<TensorPtr>& inputs) {
  std::vector<arrow::Datum> input_datums;
  input_datums.reserve(inputs.size());
  for (const auto& input : inputs) {
    input_datums.emplace_back(input->ToArrowChunkedArray());
  }

  auto status = arrow::compute::MaxElementWise(input_datums);
  YACL_ENFORCE(status.ok(),
               "caught error while invoking arrow max function: {}",
               status->ToString());

  return TensorFrom(status.ValueOrDie().chunked_array());
}

spu::Value Greatest::ComputeOnSpu(spu::SPUContext* sctx,
                                  const std::vector<spu::Value>& inputs) {
  spu::Value greatest_value = inputs[0];
  bool mix_type = false;
  spu::DataType output_type = greatest_value.dtype();
  for (size_t i = 1; i < inputs.size(); i++) {
    if (greatest_value.dtype() != inputs[i].dtype()) {
      mix_type = true;
    }

    spu::Value next_value = inputs[i];
    output_type = util::GetWiderSpuType(output_type, inputs[i].dtype());
    if (mix_type) {
      if (greatest_value.dtype() != output_type) {
        greatest_value = spu::kernel::hlo::Cast(
            sctx, greatest_value, greatest_value.vtype(), output_type);
      }

      if (next_value.dtype() != output_type) {
        next_value = spu::kernel::hlo::Cast(sctx, inputs[i], inputs[i].vtype(),
                                            output_type);
      }
    }
    greatest_value = spu::kernel::hlo::Max(sctx, greatest_value, next_value);
  }

  return greatest_value;
}

// ===========================
//   Least impl
// ===========================
const std::string Least::kOpType("Least");
const std::string& Least::Type() const { return kOpType; }

TensorPtr Least::ComputeInPlain(const std::vector<TensorPtr>& inputs) {
  std::vector<arrow::Datum> input_datums;
  input_datums.reserve(inputs.size());
  for (const auto& input : inputs) {
    input_datums.emplace_back(input->ToArrowChunkedArray());
  }

  auto status = arrow::compute::MinElementWise(input_datums);
  YACL_ENFORCE(status.ok(),
               "caught error while invoking arrow min function: {}",
               status->ToString());

  return TensorFrom(status.ValueOrDie().chunked_array());
}

spu::Value Least::ComputeOnSpu(spu::SPUContext* sctx,
                               const std::vector<spu::Value>& inputs) {
  spu::Value least_value = inputs[0];
  bool mix_type = false;
  spu::DataType output_type = least_value.dtype();
  for (size_t i = 1; i < inputs.size(); i++) {
    if (least_value.dtype() != inputs[i].dtype()) {
      mix_type = true;
    }

    spu::Value next_value = inputs[i];
    output_type = util::GetWiderSpuType(output_type, inputs[i].dtype());
    if (mix_type) {
      if (least_value.dtype() != output_type) {
        least_value = spu::kernel::hlo::Cast(sctx, least_value,
                                             least_value.vtype(), output_type);
      }
      if (next_value.dtype() != output_type) {
        next_value = spu::kernel::hlo::Cast(sctx, inputs[i], inputs[i].vtype(),
                                            output_type);
      }
    }
    least_value = spu::kernel::hlo::Min(sctx, least_value, next_value);
  }

  return least_value;
}
}  // namespace scql::engine::op