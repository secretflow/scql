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

#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "libspu/kernel/hlo/basic_binary.h"

#include "engine/core/tensor_constructor.h"
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

}  // namespace scql::engine::op