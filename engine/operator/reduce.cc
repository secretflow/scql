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
#include "libspu/kernel/hal/shape_ops.h"
#include "libspu/kernel/hlo/basic_binary.h"
#include "libspu/kernel/hlo/const.h"
#include "libspu/kernel/hlo/reduce.h"

#include "engine/core/arrow_helper.h"
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

  if (util::GetTensorStatus(input_pb) == pb::TENSORSTATUS_PRIVATE) {
    auto tensor = ctx->GetTensorTable()->GetTensor(input_pb.name());
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

    ctx->GetTensorTable()->AddTensor(output_pb.name(),
                                     std::make_shared<Tensor>(chunked_arr));
  } else {
    auto hctx = ctx->GetSession()->GetSpuHalContext();
    auto symbols = ctx->GetSession()->GetDeviceSymbols();
    auto in_value =
        symbols->getVar(util::SpuVarNameEncoder::GetValueName(input_pb.name()));
    auto out_value = SecretReduceImpl(hctx, in_value);
    symbols->setVar(util::SpuVarNameEncoder::GetValueName(output_pb.name()),
                    out_value);
#ifdef SCQL_WITH_NULL
    // TODO: complete null propagation in aggregation
#endif
  }
}

spu::Value ReduceBase::SecretReduceImpl(spu::HalContext* hctx,
                                        const spu::Value& in) {
  if (in.numel() == 0) {
    return HandleEmptyInput(in);
  }

  AggregateInit(hctx, in);

  const auto& init_value = GetInitValue(hctx);
  auto reduce_fn = GetReduceFn(hctx);

  spu::kernel::hlo::BatchedValueBinaryFn reducer =
      [reduce_fn](absl::Span<spu::Value const> lhs,
                  absl::Span<spu::Value const> rhs) {
        std::vector<spu::Value> out;
        std::transform(lhs.begin(), lhs.end(), rhs.begin(),
                       std::back_inserter(out), reduce_fn);
        return out;
      };

  auto results = spu::kernel::hlo::Reduce(hctx, std::vector<spu::Value>{in},
                                          std::vector<spu::Value>{init_value},
                                          std::vector<int64_t>{0}, reducer);

  return AggregateFinalize(hctx, results[0]);
}

// ================
//  ReduceSum impl
// ================

const std::string ReduceSum::kOpType("ReduceSum");

const std::string& ReduceSum::Type() const { return kOpType; }

spu::Value ReduceSum::GetInitValue(spu::HalContext* hctx) {
  return spu::kernel::hlo::Constant(hctx, int64_t(0), {1});
}

ReduceBase::ReduceFn ReduceSum::GetReduceFn(spu::HalContext* hctx) {
  return [hctx](const spu::Value& lhs, const spu::Value& rhs) -> spu::Value {
    return spu::kernel::hlo::Add(hctx, lhs, rhs);
  };
}

// ===============
// ReduceAvg impl
// ===============

const std::string ReduceAvg::kOpType("ReduceAvg");
const std::string& ReduceAvg::Type() const { return kOpType; }

spu::Value ReduceAvg::HandleEmptyInput(const spu::Value& in) {
  return in.clone().setDtype(spu::DT_FXP, true);
}

void ReduceAvg::AggregateInit(spu::HalContext* /*hctx*/, const spu::Value& in) {
  if (in.numel() == 0 || in.shape().size() == 0) {
    count_ = 0;
  } else {
    count_ = in.shape()[0];
  }
}

spu::Value ReduceAvg::GetInitValue(spu::HalContext* hctx) {
  return spu::kernel::hlo::Constant(hctx, 0, {1});
}

ReduceBase::ReduceFn ReduceAvg::GetReduceFn(spu::HalContext* hctx) {
  return [hctx](const spu::Value& lhs, const spu::Value& rhs) -> spu::Value {
    return spu::kernel::hlo::Add(hctx, lhs, rhs);
  };
}

spu::Value ReduceAvg::AggregateFinalize(spu::HalContext* hctx,
                                        const spu::Value& sum) {
  auto count_f = spu::kernel::hlo::Constant(hctx, count_ * 1.0, {1});
  return spu::kernel::hlo::Div(hctx, sum, count_f);
}

// =====================
// ReduceMin impl
// =====================

const std::string ReduceMin::kOpType("ReduceMin");
const std::string& ReduceMin::Type() const { return kOpType; }

void ReduceMin::AggregateInit(spu::HalContext* hctx, const spu::Value& in) {
  // set init_value_ to the first element of in
  init_value_ = spu::kernel::hal::slice(hctx, in, {0}, {1}, {});
}

spu::Value ReduceMin::GetInitValue(spu::HalContext* hctx) {
  return init_value_;
}

ReduceBase::ReduceFn ReduceMin::GetReduceFn(spu::HalContext* hctx) {
  return [hctx](const spu::Value& lhs, const spu::Value& rhs) -> spu::Value {
    return spu::kernel::hlo::Min(hctx, lhs, rhs);
  };
}

// =====================
// ReduceMax impl
// =====================

const std::string ReduceMax::kOpType("ReduceMax");
const std::string& ReduceMax::Type() const { return kOpType; }

void ReduceMax::AggregateInit(spu::HalContext* hctx, const spu::Value& in) {
  // set init_value_ to the first element of in
  init_value_ = spu::kernel::hal::slice(hctx, in, {0}, {1}, {});
}

spu::Value ReduceMax::GetInitValue(spu::HalContext* hctx) {
  return init_value_;
}

ReduceBase::ReduceFn ReduceMax::GetReduceFn(spu::HalContext* hctx) {
  return [hctx](const spu::Value& lhs, const spu::Value& rhs) -> spu::Value {
    return spu::kernel::hlo::Max(hctx, lhs, rhs);
  };
}

}  // namespace scql::engine::op