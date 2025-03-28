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

#include "engine/operator/group_agg.h"

#include "arrow/compute/api.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/row/grouper.h"
#include "arrow/scalar.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor_constructor.h"
#include "engine/core/type.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

void GroupAggBase::Validate(ExecContext* ctx) {
  const auto& group_id = ctx->GetInput(kGroupId);
  YACL_ENFORCE(group_id.size() == 1, "group id size must be 1");
  const auto& group_num = ctx->GetInput(kGroupNum);
  YACL_ENFORCE(group_num.size() == 1, "group num size must be 1");
  const auto& inputs = ctx->GetInput(kIn);
  YACL_ENFORCE(inputs.size() > 0, "input size cannot be 0");
  const auto& outputs = ctx->GetOutput(kOut);
  YACL_ENFORCE(outputs.size() == inputs.size(),
               "outputs' size={} not equal to inputs' size={}", outputs.size(),
               inputs.size());

  YACL_ENFORCE(
      util::IsTensorStatusMatched(group_id[0], pb::TENSORSTATUS_PRIVATE),
      "group id status is not private");
  YACL_ENFORCE(
      util::IsTensorStatusMatched(group_num[0], pb::TENSORSTATUS_PRIVATE),
      "group num status is not private");
  YACL_ENFORCE(util::AreTensorsStatusMatched(inputs, pb::TENSORSTATUS_PRIVATE),
               "inputs' status are not all private");
  YACL_ENFORCE(util::AreTensorsStatusMatched(outputs, pb::TENSORSTATUS_PRIVATE),
               "outputs' status  are not all private");
}

void GroupAggBase::Execute(ExecContext* ctx) {
  auto group_array = GetGroupId(ctx);
  const auto* group_cast =
      dynamic_cast<const arrow::UInt32Array*>(group_array.get());
  YACL_ENFORCE(group_cast, "cast group id to uint32_t failed");
  std::shared_ptr<arrow::ListArray> groupings;
  ASSIGN_OR_THROW_ARROW_STATUS(
      groupings,
      arrow::compute::Grouper::MakeGroupings(*group_cast, GetGroupNum(ctx)));

  const auto& input_pbs = ctx->GetInput(kIn);
  const auto& output_pbs = ctx->GetOutput(kOut);
  for (int i = 0; i < input_pbs.size(); ++i) {
    const auto& input_pb = input_pbs[i];
    auto in_t = ctx->GetTensorTable()->GetTensor(input_pb.name());
    YACL_ENFORCE(in_t, "no input tensor={}", input_pb.name());

    auto in_array = util::ConcatenateChunkedArray(in_t->ToArrowChunkedArray());

    std::shared_ptr<arrow::ListArray> grouped_in;
    ASSIGN_OR_THROW_ARROW_STATUS(
        grouped_in,
        arrow::compute::Grouper::ApplyGroupings(*groupings, *in_array));

    arrow::ScalarVector aggregated_scalars;
    for (uint32_t group_i = 0; group_i < GetGroupNum(ctx); ++group_i) {
      auto slice = grouped_in->value_slice(group_i);
      if (slice->length() == 0) {
        continue;
      }
      aggregated_scalars.push_back(AggImpl(ctx, slice));
    }

    auto t = BuildTensorFromScalarVector(
        aggregated_scalars, ToArrowDataType(output_pbs[i].elem_type()));
    YACL_ENFORCE(t);
    ctx->GetTensorTable()->AddTensor(output_pbs[i].name(), t);
  }
}

uint32_t GroupAggBase::GetGroupNum(ExecContext* ctx) {
  const auto& group_num = ctx->GetInput(kGroupNum)[0];
  auto group_num_t = ctx->GetTensorTable()->GetTensor(group_num.name());
  YACL_ENFORCE(group_num_t, "no group_num={}", group_num.name());
  auto num_array =
      util::ConcatenateChunkedArray(group_num_t->ToArrowChunkedArray());
  const auto* num_array_ptr =
      dynamic_cast<const arrow::UInt32Array*>(num_array.get());
  YACL_ENFORCE(num_array_ptr, "cast group num to uint32_t failed");
  return num_array_ptr->Value(0);
}

std::shared_ptr<arrow::Array> GroupAggBase::GetGroupId(ExecContext* ctx) {
  const auto& group = ctx->GetInput(kGroupId)[0];
  auto group_t = ctx->GetTensorTable()->GetTensor(group.name());
  YACL_ENFORCE(group_t, "no group id={}", group.name());
  return util::ConcatenateChunkedArray(group_t->ToArrowChunkedArray());
}

TensorPtr GroupAggBase::BuildTensorFromScalarVector(
    const arrow::ScalarVector& scalars,
    std::shared_ptr<arrow::DataType> empty_type) {
  std::unique_ptr<arrow::ArrayBuilder> builder;
  std::shared_ptr<arrow::DataType> type =
      scalars.size() > 0 ? scalars[0]->type : empty_type;
  YACL_ENFORCE(type, "no arrow type for proto type={}", type->ToString());
  THROW_IF_ARROW_NOT_OK(
      arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder));

  THROW_IF_ARROW_NOT_OK(builder->AppendScalars(scalars));

  std::shared_ptr<arrow::Array> array;
  THROW_IF_ARROW_NOT_OK(builder->Finish(&array));

  auto chunked_arr = std::make_shared<arrow::ChunkedArray>(array);
  return TensorFrom(chunked_arr);
}

// ===========================
//   GroupFirstOf impl
// ===========================

const std::string GroupFirstOf::kOpType("GroupFirstOf");

std::shared_ptr<arrow::Scalar> GroupFirstOf::AggImpl(
    ExecContext* ctx, std::shared_ptr<arrow::Array> arr) {
  std::shared_ptr<arrow::Scalar> ret;
  ASSIGN_OR_THROW_ARROW_STATUS(ret, arr->GetScalar(0));
  return ret;
}

// ===========================
//   GroupCountDistinct impl
// ===========================

const std::string GroupCountDistinct::kOpType("GroupCountDistinct");

std::shared_ptr<arrow::Scalar> GroupCountDistinct::AggImpl(
    ExecContext* ctx, std::shared_ptr<arrow::Array> arr) {
  auto ret = arrow::compute::CallFunction("count_distinct", {arr});
  YACL_ENFORCE(ret.ok(),
               "invoking arrow function 'count_distinct' failed: err_msg = {} ",
               ret.status().ToString());
  return ret.ValueOrDie().scalar();
}

// ===========================
//   GroupCount impl
// ===========================

const std::string GroupCount::kOpType("GroupCount");

std::shared_ptr<arrow::Scalar> GroupCount::AggImpl(
    ExecContext* ctx, std::shared_ptr<arrow::Array> arr) {
  auto ret = arrow::compute::CallFunction("count", {arr});
  YACL_ENFORCE(ret.ok(),
               "invoking arrow function 'count' failed: err_msg = {} ",
               ret.status().ToString());
  return ret.ValueOrDie().scalar();
}

// ===========================
//   GroupSum impl
// ===========================

const std::string GroupSum::kOpType("GroupSum");

std::shared_ptr<arrow::Scalar> GroupSum::AggImpl(
    ExecContext* ctx, std::shared_ptr<arrow::Array> arr) {
  auto ret = arrow::compute::CallFunction("sum", {arr});
  YACL_ENFORCE(ret.ok(), "invoking arrow function 'sum' failed: err_msg = {} ",
               ret.status().ToString());
  return ret.ValueOrDie().scalar();
}

// ===========================
//   GroupAvg impl
// ===========================

const std::string GroupAvg::kOpType("GroupAvg");

std::shared_ptr<arrow::Scalar> GroupAvg::AggImpl(
    ExecContext* ctx, std::shared_ptr<arrow::Array> arr) {
  auto ret = arrow::compute::CallFunction("mean", {arr});
  YACL_ENFORCE(ret.ok(), "invoking arrow function 'mean' failed: err_msg = {} ",
               ret.status().ToString());
  return ret.ValueOrDie().scalar();
}

// ===========================
//   GroupMin impl
// ===========================

const std::string GroupMin::kOpType("GroupMin");

std::shared_ptr<arrow::Scalar> GroupMin::AggImpl(
    ExecContext* ctx, std::shared_ptr<arrow::Array> arr) {
  auto ret = arrow::compute::CallFunction("min", {arr});
  YACL_ENFORCE(ret.ok(), "invoking arrow function 'min' failed: err_msg = {} ",
               ret.status().ToString());
  return ret.ValueOrDie().scalar();
}

// ===========================
//   GroupMax impl
// ===========================

const std::string GroupMax::kOpType("GroupMax");

std::shared_ptr<arrow::Scalar> GroupMax::AggImpl(
    ExecContext* ctx, std::shared_ptr<arrow::Array> arr) {
  auto ret = arrow::compute::CallFunction("max", {arr});
  YACL_ENFORCE(ret.ok(), "invoking arrow function 'max' failed: err_msg = {} ",
               ret.status().ToString());
  return ret.ValueOrDie().scalar();
}

// ===========================
//   GroupPercentileDisc impl
// ===========================
const std::string GroupPercentileDisc::kOpType("GroupPercentileDisc");

std::shared_ptr<arrow::Scalar> GroupPercentileDisc::AggImpl(
    ExecContext* ctx, std::shared_ptr<arrow::Array> arr) {
  YACL_ENFORCE(arr != nullptr && arr->length() > 0,
               "input array should not be null or empty");
  double percent = ctx->GetDoubleValueFromAttribute(kPercent);
  YACL_ENFORCE(percent >= 0.0 && percent <= 1.0,
               "percent should be in [0.0, 1.0], but got {}", percent);
  int64_t length = arr->length();

  int pos =
      static_cast<int>(std::ceil(percent * static_cast<double>(length)) - 1);
  pos = std::max(pos, 0);
  pos = std::min(pos, static_cast<int>(length - 1));

  arrow::compute::PartitionNthOptions options(pos);
  auto result =
      arrow::compute::CallFunction("partition_nth_indices", {arr}, &options);

  YACL_ENFORCE(result.ok(),
               "failed to call partition_nth_indices function, error = {}",
               result.status().ToString());
  auto partitioned_indices = std::static_pointer_cast<arrow::UInt64Array>(
      result.ValueOrDie().make_array());
  auto value = arr->GetScalar(partitioned_indices->Value(pos));
  YACL_ENFORCE(value.ok(), "failed to get scalar from array, error = {}",
               value.status().ToString());
  return value.ValueOrDie();
}
};  // namespace scql::engine::op