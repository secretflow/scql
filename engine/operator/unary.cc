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

#include "engine/operator/unary.h"

#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "libspu/kernel/hlo/basic_binary.h"
#include "libspu/kernel/hlo/basic_unary.h"
#include "libspu/kernel/hlo/const.h"

#include "engine/core/tensor_constructor.h"

namespace scql::engine::op {

const std::string Abs::kOpType("Abs");
// ===========================
//   Abs impl
// ===========================
const std::string& Abs::Type() const { return kOpType; }
TensorPtr Abs::ComputeInPlain(const Tensor& in) {
  arrow::Result<arrow::Datum> result =
      arrow::compute::AbsoluteValue(in.ToArrowChunkedArray());
  YACL_ENFORCE(result.ok(), "failed to compute abs");
  return TensorFrom(result.ValueOrDie().chunked_array());
}

spu::Value Abs::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& in) {
  return spu::kernel::hlo::Abs(sctx, in);
}

// ===========================
//   Ceil impl
// ===========================
const std::string Ceil::kOpType("Ceil");

const std::string& Ceil::Type() const { return kOpType; }

TensorPtr Ceil::ComputeInPlain(const Tensor& in) {
  arrow::Result<arrow::Datum> result =
      arrow::compute::Ceil(in.ToArrowChunkedArray());
  YACL_ENFORCE(result.ok(), "failed to compute ceil");
  SPDLOG_INFO("after compute ceil");
  return TensorFrom(result.ValueOrDie().chunked_array());
}

spu::Value Ceil::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& in) {
  return spu::kernel::hlo::Ceil(sctx, in);
}

// ===========================
//   Floor impl
// ===========================
const std::string Floor::kOpType("Floor");

const std::string& Floor::Type() const { return kOpType; }

TensorPtr Floor::ComputeInPlain(const Tensor& in) {
  arrow::Result<arrow::Datum> result =
      arrow::compute::Floor(in.ToArrowChunkedArray());
  YACL_ENFORCE(result.ok(), "failed to compute floor");
  return TensorFrom(result.ValueOrDie().chunked_array());
}

spu::Value Floor::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& in) {
  return spu::kernel::hlo::Floor(sctx, in);
}

// ===========================
//   Round impl
// ===========================
const std::string Round::kOpType("Round");
const std::string& Round::Type() const { return kOpType; }
TensorPtr Round::ComputeInPlain(const Tensor& in) {
  arrow::Result<arrow::Datum> result = arrow::compute::Round(
      in.ToArrowChunkedArray(), arrow::compute::RoundOptions(0));
  YACL_ENFORCE(result.ok(), "failed to compute round");
  return TensorFrom(result.ValueOrDie().chunked_array());
}

spu::Value Round::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& in) {
  YACL_THROW("spu not support round yet.");
}

// ===========================
//   Radians impl
// ===========================
const std::string Radians::kOpType("Radians");
const std::string& Radians::Type() const { return kOpType; }

TensorPtr Radians::ComputeInPlain(const Tensor& in) {
  const double radians_factor = M_PI / 180.0;

  auto in_chunk = in.ToArrowChunkedArray();
  arrow::Result<arrow::Datum> result =
      arrow::compute::Multiply(in_chunk, arrow::Datum(radians_factor));
  YACL_ENFORCE(result.ok(), "failed to compute radians, {}",
               result.status().ToString());
  return TensorFrom(result.ValueOrDie().chunked_array());
}

spu::Value Radians::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& in) {
  const double radians_factor = M_PI / 180.0;
  spu::Value radians_factor_value =
      spu::kernel::hlo::Constant(sctx, radians_factor, in.shape());

  return spu::kernel::hlo::Mul(sctx, in, radians_factor_value);
}
// ===========================
//   Degrees impl
// ===========================
const std::string Degrees::kOpType("Degrees");
const std::string& Degrees::Type() const { return kOpType; }

TensorPtr Degrees::ComputeInPlain(const Tensor& in) {
  const double degrees_factor = 180.0 / M_PI;

  auto in_chunk = in.ToArrowChunkedArray();
  arrow::Result<arrow::Datum> result =
      arrow::compute::Multiply(in_chunk, arrow::Datum(degrees_factor));
  YACL_ENFORCE(result.ok(), "failed to compute radians, {}",
               result.status().ToString());
  return TensorFrom(result.ValueOrDie().chunked_array());
}

spu::Value Degrees::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& in) {
  const double degrees_factor = 180.0 / M_PI;
  spu::Value radians_factor_value =
      spu::kernel::hlo::Constant(sctx, degrees_factor, in.shape());

  return spu::kernel::hlo::Mul(sctx, in, radians_factor_value);
}

// ===========================
//   Ln impl
// ===========================
const std::string Ln::kOpType("Ln");
const std::string& Ln::Type() const { return kOpType; }

TensorPtr Ln::ComputeInPlain(const Tensor& in) {
  arrow::Result<arrow::Datum> result =
      arrow::compute::Ln(in.ToArrowChunkedArray());
  YACL_ENFORCE(result.ok(), "failed to compute ln");
  return TensorFrom(result.ValueOrDie().chunked_array());
}

spu::Value Ln::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& in) {
  return spu::kernel::hlo::Log(sctx, in);
}

// ===========================
//   Log10 impl
// ===========================
const std::string Log10::kOpType("Log10");
const std::string& Log10::Type() const { return kOpType; }

TensorPtr Log10::ComputeInPlain(const Tensor& in) {
  arrow::Result<arrow::Datum> result =
      arrow::compute::Log10(in.ToArrowChunkedArray());
  YACL_ENFORCE(result.ok(), "failed to compute log10");
  return TensorFrom(result.ValueOrDie().chunked_array());
}

spu::Value Log10::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& in) {
  YACL_THROW("spu not support log10 yet.");
}

// ===========================
//   Log2 impl
// ===========================
const std::string Log2::kOpType("Log2");
const std::string& Log2::Type() const { return kOpType; }

TensorPtr Log2::ComputeInPlain(const Tensor& in) {
  arrow::Result<arrow::Datum> result =
      arrow::compute::Log2(in.ToArrowChunkedArray());
  YACL_ENFORCE(result.ok(), "failed to compute log2");
  return TensorFrom(result.ValueOrDie().chunked_array());
}

spu::Value Log2::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& in) {
  YACL_THROW("spu not support log2 yet.");
}

// ===========================
//   Sqrt impl
// ===========================
const std::string Sqrt::kOpType("Sqrt");
const std::string& Sqrt::Type() const { return kOpType; }

TensorPtr Sqrt::ComputeInPlain(const Tensor& in) {
  arrow::Result<arrow::Datum> result =
      arrow::compute::Sqrt(in.ToArrowChunkedArray());
  YACL_ENFORCE(result.ok(), "failed to compute sqrt");
  return TensorFrom(result.ValueOrDie().chunked_array());
}

spu::Value Sqrt::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& in) {
  return spu::kernel::hlo::Sqrt(sctx, in);
}

// ===========================
//   Exp impl
// ===========================
const std::string Exp::kOpType("Exp");
const std::string& Exp::Type() const { return kOpType; }

TensorPtr Exp::ComputeInPlain(const Tensor& in) {
  arrow::Result<arrow::Datum> result =
      arrow::compute::Exp(in.ToArrowChunkedArray());
  YACL_ENFORCE(result.ok(), "failed to compute exp");
  return TensorFrom(result.ValueOrDie().chunked_array());
}

spu::Value Exp::ComputeOnSpu(spu::SPUContext* sctx, const spu::Value& in) {
  return spu::kernel::hlo::Exp(sctx, in);
}
}  // namespace scql::engine::op