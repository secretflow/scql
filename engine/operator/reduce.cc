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

#include "engine/operator/reduce.h"

#include <functional>
#include <iterator>

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/exec.h"
#include "libspu/kernel/hal/constants.h"
#include "libspu/kernel/hal/shape_ops.h"
#include "libspu/kernel/hlo/basic_binary.h"
#include "libspu/kernel/hlo/const.h"
#include "libspu/kernel/hlo/indexing.h"
#include "libspu/kernel/hlo/reduce.h"
#include "libspu/kernel/hlo/sort.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor_constructor.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

void ReduceBase::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  const auto& outputs = ctx->GetOutput(kOut);

  YACL_ENFORCE(inputs.size() == 1,
               "operator {} input size shoule be 1, but got={}", Type(),
               inputs.size());
  YACL_ENFORCE(outputs.size() == 1,
               "operator {} output size shoule be 1, but got={}", Type(),
               outputs.size());

  const auto& input_status = util::GetTensorStatus(inputs[0]);
  YACL_ENFORCE(input_status == pb::TENSORSTATUS_PRIVATE ||
               input_status == pb::TENSORSTATUS_SECRET);
  YACL_ENFORCE(util::IsTensorStatusMatched(outputs[0], input_status));
}

void ReduceBase::Execute(ExecContext* ctx) {
  const auto& input_pb = ctx->GetInput(kIn)[0];
  const auto& output_pb = ctx->GetOutput(kOut)[0];

  InitAttribute(ctx);
  if (util::GetTensorStatus(input_pb) == pb::TENSORSTATUS_PRIVATE) {
    ExecuteInPlain(ctx, input_pb, output_pb);
  } else {
    auto* sctx = ctx->GetSession()->GetSpuContext();
    auto* symbols = ctx->GetSession()->GetDeviceSymbols();
    auto in_value =
        symbols->getVar(util::SpuVarNameEncoder::GetValueName(input_pb.name()));
    auto out_value = SecretReduceImpl(sctx, in_value);
    symbols->setVar(util::SpuVarNameEncoder::GetValueName(output_pb.name()),
                    out_value);
#ifdef SCQL_WITH_NULL
    // TODO: complete null propagation in aggregation
#endif
  }
}

void ReduceBase::ExecuteInPlain(ExecContext* ctx, const pb::Tensor& input_pb,
                                const pb::Tensor& output_pb) {
  const auto tensor = ctx->GetTensorTable()->GetTensor(input_pb.name());
  YACL_ENFORCE(tensor, "get private tensor failed, name={}", input_pb.name());

  const std::string& arrow_fun_name = GetArrowFunName();
  auto result = arrow::compute::CallFunction(arrow_fun_name,
                                             {tensor->ToArrowChunkedArray()});
  YACL_ENFORCE(result.ok(), "invoking arrow function '{}' failed: err_msg={}",
               arrow_fun_name, result.status().ToString());
  auto scalar = result.ValueOrDie().scalar();
  std::shared_ptr<arrow::Array> array;
  ASSIGN_OR_THROW_ARROW_STATUS(array, arrow::MakeArrayFromScalar(*scalar, 1));
  auto chunked_arr = std::make_shared<arrow::ChunkedArray>(array);

  ctx->GetTensorTable()->AddTensor(output_pb.name(), TensorFrom(chunked_arr));
}

spu::Value ReduceBase::SecretReduceImpl(spu::SPUContext* sctx,
                                        const spu::Value& in) {
  if (in.numel() == 0) {
    return HandleEmptyInput(in);
  }

  AggregateInit(sctx, in);

  const auto& init_value = GetInitValue(sctx);
  auto reduce_fn = GetReduceFn(sctx);

  spu::kernel::hlo::BatchedValueBinaryFn reducer =
      [reduce_fn](absl::Span<spu::Value const> lhs,
                  absl::Span<spu::Value const> rhs) {
        std::vector<spu::Value> out;
        std::transform(lhs.begin(), lhs.end(), rhs.begin(),
                       std::back_inserter(out), reduce_fn);
        return out;
      };

  auto results = spu::kernel::hlo::Reduce(sctx, std::vector<spu::Value>{in},
                                          std::vector<spu::Value>{init_value},
                                          spu::Axes{0}, reducer);

  return AggregateFinalize(sctx, results[0]);
}

// ================
//  ReduceSum impl
// ================

const std::string ReduceSum::kOpType("ReduceSum");

const std::string& ReduceSum::Type() const { return kOpType; }

spu::Value ReduceSum::GetInitValue(spu::SPUContext* sctx) {
  return spu::kernel::hlo::Constant(sctx, static_cast<int64_t>(0), {1});
}

ReduceBase::ReduceFn ReduceSum::GetReduceFn(spu::SPUContext* sctx) {
  return [sctx](const spu::Value& lhs, const spu::Value& rhs) -> spu::Value {
    return spu::kernel::hlo::Add(sctx, lhs, rhs);
  };
}

// ================
//  ReduceCount impl
// ================

const std::string ReduceCount::kOpType("ReduceCount");

const std::string& ReduceCount::Type() const { return kOpType; }

spu::Value ReduceCount::GetInitValue(spu::SPUContext* sctx) {
  YACL_THROW("unsupported reduce func: count");
}

ReduceBase::ReduceFn ReduceCount::GetReduceFn(spu::SPUContext* sctx) {
  YACL_THROW("unsupported reduce func: count");
}

// ===============
// ReduceAvg impl
// ===============

const std::string ReduceAvg::kOpType("ReduceAvg");
const std::string& ReduceAvg::Type() const { return kOpType; }

spu::Value ReduceAvg::HandleEmptyInput(const spu::Value& in) {
  return in.clone().setDtype(spu::DT_F64, true);
}

void ReduceAvg::AggregateInit(spu::SPUContext* /*sctx*/, const spu::Value& in) {
  if (in.numel() == 0 || in.shape().size() == 0) {
    count_ = 0;
  } else {
    count_ = in.shape()[0];
  }
}

spu::Value ReduceAvg::GetInitValue(spu::SPUContext* sctx) {
  return spu::kernel::hlo::Constant(sctx, 0, {1});
}

ReduceBase::ReduceFn ReduceAvg::GetReduceFn(spu::SPUContext* sctx) {
  return [sctx](const spu::Value& lhs, const spu::Value& rhs) -> spu::Value {
    return spu::kernel::hlo::Add(sctx, lhs, rhs);
  };
}

spu::Value ReduceAvg::AggregateFinalize(spu::SPUContext* sctx,
                                        const spu::Value& sum) {
  auto count_f = spu::kernel::hlo::Constant(sctx, count_ * 1.0, {1});
  return spu::kernel::hlo::Div(sctx, sum, count_f);
}

// =====================
// ReduceMin impl
// =====================

const std::string ReduceMin::kOpType("ReduceMin");
const std::string& ReduceMin::Type() const { return kOpType; }

void ReduceMin::AggregateInit(spu::SPUContext* sctx, const spu::Value& in) {
  // set init_value_ to the first element of in
  init_value_ = spu::kernel::hal::slice(sctx, in, {0}, {1}, {});
}

spu::Value ReduceMin::GetInitValue(spu::SPUContext* sctx) {
  return init_value_;
}

ReduceBase::ReduceFn ReduceMin::GetReduceFn(spu::SPUContext* sctx) {
  return [sctx](const spu::Value& lhs, const spu::Value& rhs) -> spu::Value {
    return spu::kernel::hlo::Min(sctx, lhs, rhs);
  };
}

// =====================
// ReduceMax impl
// =====================

const std::string ReduceMax::kOpType("ReduceMax");
const std::string& ReduceMax::Type() const { return kOpType; }

void ReduceMax::AggregateInit(spu::SPUContext* sctx, const spu::Value& in) {
  // set init_value_ to the first element of in
  init_value_ = spu::kernel::hal::slice(sctx, in, {0}, {1}, {});
}

spu::Value ReduceMax::GetInitValue(spu::SPUContext* sctx) {
  return init_value_;
}

ReduceBase::ReduceFn ReduceMax::GetReduceFn(spu::SPUContext* sctx) {
  return [sctx](const spu::Value& lhs, const spu::Value& rhs) -> spu::Value {
    return spu::kernel::hlo::Max(sctx, lhs, rhs);
  };
}

// =====================
// ReducePercentileDisc impl
// =====================

const std::string ReducePercentileDisc::kOpType("ReducePercentileDisc");
const std::string& ReducePercentileDisc::Type() const { return kOpType; }

void ReducePercentileDisc::InitAttribute(ExecContext* ctx) {
  percent_ = ctx->GetDoubleValueFromAttribute(kPercent);
  YACL_ENFORCE(percent_ >= 0 && percent_ <= 1,
               "percent should be in [0, 1], but got={}", percent_);
}

void ReducePercentileDisc::AggregateInit(spu::SPUContext* sctx,
                                         const spu::Value& in) {
  // do nothing
}

spu::Value ReducePercentileDisc::GetInitValue(spu::SPUContext* sctx) {
  YACL_THROW("should not reach here");
}

ReduceBase::ReduceFn ReducePercentileDisc::GetReduceFn(spu::SPUContext* sctx) {
  YACL_THROW("should not reach here");
}

void ReducePercentileDisc::ExecuteInPlain(ExecContext* ctx,
                                          const pb::Tensor& in,
                                          const pb::Tensor& out) {
  auto tensor = ctx->GetTensorTable()->GetTensor(in.name());
  YACL_ENFORCE(tensor, "get private tensor failed, name={}", in.name());
  if (tensor->Length() == 0) {
    ctx->GetTensorTable()->AddTensor(out.name(), tensor);
    return;
  }
  int64_t pos = GetPointIndex(tensor->Length());
  arrow::compute::PartitionNthOptions options(pos);

  const arrow::compute::FunctionOptions* options_ptr = &options;
  std::shared_ptr<arrow::Array> array;
  ASSIGN_OR_THROW_ARROW_STATUS(
      array, arrow::Concatenate(tensor->ToArrowChunkedArray()->chunks(),
                                arrow::default_memory_pool()));
  auto result = arrow::compute::CallFunction("partition_nth_indices", {array},
                                             options_ptr);

  YACL_ENFORCE(result.ok(),
               "invoking arrow function 'partition_nth_indices' "
               "failed: err_msg={}",
               result.status().ToString());
  auto partitioned_indices = std::static_pointer_cast<arrow::UInt64Array>(
      result.ValueOrDie().make_array());
  std::shared_ptr<arrow::Scalar> scalar;
  ASSIGN_OR_THROW_ARROW_STATUS(
      scalar, array->GetScalar(partitioned_indices->Value(pos)));
  ASSIGN_OR_THROW_ARROW_STATUS(array, arrow::MakeArrayFromScalar(*scalar, 1));
  auto chunked_arr = std::make_shared<arrow::ChunkedArray>(array);

  ctx->GetTensorTable()->AddTensor(out.name(), TensorFrom(chunked_arr));
}

spu::Value ReducePercentileDisc::SecretReduceImpl(spu::SPUContext* sctx,
                                                  const spu::Value& in) {
  auto length = in.shape()[0];
  if (length == 0) {
    return in;
  }

  auto pos =
      static_cast<int>(std::ceil(percent_ * static_cast<double>(length)) - 1);
  pos = std::max(pos, 0);
  pos = std::min(pos, static_cast<int>(length - 1));

  auto sorted = spu::kernel::hlo::SimpleSort(
      sctx, {in}, 0, spu::kernel::hal::SortDirection::Ascending, 1);
  spu::Value left_zeros = spu::kernel::hlo::Constant(sctx, 0, {pos});
  spu::Value right_zeros =
      spu::kernel::hlo::Constant(sctx, 0, {length - pos - 1});
  spu::Value concat = spu::kernel::hlo::Concatenate(
      sctx, {left_zeros, spu::kernel::hlo::Constant(sctx, 1, {1}), right_zeros},
      0);
  auto gathered = spu::kernel::hlo::Mul(sctx, sorted[0], concat);

  spu::kernel::hlo::BatchedValueBinaryFn reducer =
      [sctx](absl::Span<spu::Value const> lhs,
             absl::Span<spu::Value const> rhs) {
        std::vector<spu::Value> out;
        std::transform(lhs.begin(), lhs.end(), rhs.begin(),
                       std::back_inserter(out),
                       [sctx](const spu::Value& lhs, const spu::Value& rhs) {
                         return spu::kernel::hlo::Add(sctx, lhs, rhs);
                       });
        return out;
      };
  auto zero = spu::kernel::hlo::Constant(sctx, 0, {1});
  auto result = spu::kernel::hlo::Reduce(
      sctx, std::vector<spu::Value>{gathered}, std::vector<spu::Value>{zero},
      spu::Axes{0}, reducer);
  return result[0];
}
}  // namespace scql::engine::op