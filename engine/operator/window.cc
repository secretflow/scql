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

#include "engine/operator/window.h"

#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "arrow/compute/row/grouper.h"
#include "arrow/result.h"
#include "arrow/table.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor_constructor.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {
const std::string RowNumber::kOpType("RowNumber");
const std::string& RowNumber::Type() const { return RowNumber::kOpType; }

void RankWindowBase::Validate(ExecContext* ctx) {
  const auto& partition_id = ctx->GetInput(kPartitionId);
  YACL_ENFORCE(partition_id.size() == 1, "partition id size must be 1");
  const auto& partition_num = ctx->GetInput(kPartitionNum);
  YACL_ENFORCE(partition_num.size() == 1, "partition num size must be 1");
  const auto& inputs = ctx->GetInput(kIn);
  YACL_ENFORCE(inputs.size() > 0, "input size can't be 0");
  const auto& outputs = ctx->GetOutput(kOut);
  YACL_ENFORCE(outputs.size() == 1, "outputs' size = {} not equal 1",
               outputs.size());

  YACL_ENFORCE(
      util::IsTensorStatusMatched(partition_id[0], pb::TENSORSTATUS_PRIVATE),
      "partition id status is not private");
  YACL_ENFORCE(
      util::IsTensorStatusMatched(partition_num[0], pb::TENSORSTATUS_PRIVATE),
      "partition num status is not private");
  YACL_ENFORCE(util::AreTensorsStatusMatched(inputs, pb::TENSORSTATUS_PRIVATE),
               "inputs' status are not all private");
  YACL_ENFORCE(util::AreTensorsStatusMatched(outputs, pb::TENSORSTATUS_PRIVATE),
               "outputs' status are not all private");
}
uint32_t RankWindowBase::GetPartitionNum(ExecContext* ctx) {
  const auto& partition_num = ctx->GetInput(kPartitionNum)[0];
  auto partition_num_t = ctx->GetTensorTable()->GetTensor(partition_num.name());
  YACL_ENFORCE(partition_num_t, "tensor({}) not found", partition_num.name());
  return util::GetScalarUint32(partition_num_t);
}

std::shared_ptr<arrow::Array> RankWindowBase::GetPartitionId(ExecContext* ctx) {
  const auto& partition = ctx->GetInput(kPartitionId)[0];
  auto partition_t = ctx->GetTensorTable()->GetTensor(partition.name());
  YACL_ENFORCE(partition_t, "tensor({}) not found", partition.name());

  return util::ConcatenateChunkedArray(partition_t->ToArrowChunkedArray());
}

std::vector<std::shared_ptr<arrow::ListArray>>
RankWindowBase::GetPartitionedInputs(ExecContext* ctx,
                                     const arrow::UInt32Array* partition_id,
                                     uint32_t partition_num) {
  // TODO: handle the special case of the partition key is empty
  std::vector<std::shared_ptr<arrow::ListArray>> partitioned_inputs;
  std::shared_ptr<arrow::ListArray> partitions;
  ASSIGN_OR_THROW_ARROW_STATUS(
      partitions,
      arrow::compute::Grouper::MakeGroupings(*partition_id, partition_num));
  const auto& input_pbs = ctx->GetInput(kIn);
  for (int i = 0; i < input_pbs.size(); i++) {
    const auto& input_pb = input_pbs[i];
    auto in_t = ctx->GetTensorTable()->GetTensor(input_pb.name());

    YACL_ENFORCE(in_t, "tensor({}) not found", input_pb.name());
    auto in_array = util::ConcatenateChunkedArray(in_t->ToArrowChunkedArray());

    std::shared_ptr<arrow::ListArray> partitioned_in;
    ASSIGN_OR_THROW_ARROW_STATUS(
        partitioned_in,
        arrow::compute::Grouper::ApplyGroupings(*partitions, *in_array));
    partitioned_inputs.push_back(partitioned_in);
  }

  return partitioned_inputs;
}

void RankWindowBase::ExecuteInPlain(ExecContext* ctx) {
  auto partition_array = GetPartitionId(ctx);
  const auto* partition_cast =
      dynamic_cast<const arrow::UInt32Array*>(partition_array.get());
  YACL_ENFORCE(partition_cast, "cast partition id to uint32_t failed");

  int64_t tensor_length = partition_cast->length();
  ReserveWindowResult(tensor_length);
  std::unordered_map<uint, std::vector<int64_t>> positions_map;
  for (int i = 0; i < tensor_length; i++) {
    // partition_cast->Value(i) is the partition group
    // positions_map stores the position info of each group
    positions_map[partition_cast->Value(i)].push_back(i);
  }

  auto partition_num = GetPartitionNum(ctx);
  const auto& input_pbs = ctx->GetInput(kIn);

  std::vector<std::shared_ptr<arrow::ListArray>> partitioned_inputs =
      GetPartitionedInputs(ctx, partition_cast, partition_num);
  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (int i = 0; i < input_pbs.size(); i++) {
    const auto& input_pb = input_pbs[i];
    auto in_t = ctx->GetTensorTable()->GetTensor(input_pb.name());
    YACL_ENFORCE(in_t, "failed to find tensor '{}'", input_pb.name());
    fields.push_back(arrow::field(input_pb.name(), in_t->ArrowType()));
  }
  std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);

  std::vector<std::shared_ptr<arrow::Array>> window_results;

  for (uint32_t partition_i = 0; partition_i < partition_num; partition_i++) {
    std::vector<std::string> sort_keys;
    std::vector<std::shared_ptr<arrow::Array>> slices;

    for (const auto& tensor_slice : partitioned_inputs) {
      std::shared_ptr<arrow::Array> partitioned_slice =
          tensor_slice->value_slice(partition_i);
      YACL_ENFORCE(partitioned_slice->length() > 0,
                   "partitioned slice(partition id: {}) is empty", partition_i);
      slices.push_back(partitioned_slice);
    }

    std::shared_ptr<arrow::Table> slice_table;

    slice_table = arrow::Table::Make(schema, slices);
    RunWindowFunc(ctx, std::move(slice_table), positions_map[partition_i]);
  }

  const auto& output_pbs = ctx->GetOutput(kOut);
  ctx->GetTensorTable()->AddTensor(output_pbs[0].name(),
                                   TensorFrom(GetWindowResult()));
  SPDLOG_INFO("{} window function done", Type());
}

void RankWindowBase::Execute(ExecContext* ctx) {
  const auto output_status = util::GetTensorStatus(ctx->GetOutput(kOut)[0]);
  if (output_status == pb::TENSORSTATUS_PRIVATE) {
    ExecuteInPlain(ctx);
  } else {
    ExecuteInSecret(ctx);
  }
}

// sort_indices is the position indices after sort, e.g. the result [3,0,2,1]
// means the 4th row is ranked as first, first row ranked as
// second, 3rd row is ranked 3rd, second row is ranked 4th
std::shared_ptr<arrow::Array> RankWindowBase::GetSortedIndices(
    ExecContext* ctx, const std::shared_ptr<arrow::Table>& input) {
  std::vector<arrow::compute::SortKey> sort_keys;

  std::vector<std::string> reverse =
      ctx->GetStringValuesFromAttribute(kReverseAttr);
  YACL_ENFORCE(reverse.size() == input->fields().size(),
               "sort key numbers are not equal to that of reverse attribute");

  for (size_t i = 0; i < input->fields().size(); i++) {
    arrow::compute::SortOrder order =
        reverse[i] == "1" ? arrow::compute::SortOrder::Descending
                          : arrow::compute::SortOrder::Ascending;
    sort_keys.emplace_back(input->fields()[i]->name(), order);
  }

  arrow::compute::SortOptions sort_options(sort_keys);
  auto status = arrow::compute::SortIndices(input, sort_options);
  YACL_ENFORCE(status.ok(), "failed to sort indices");
  return status.ValueOrDie();
}

void RowNumber::RunWindowFunc(ExecContext* ctx,
                              std::shared_ptr<arrow::Table> input,
                              const std::vector<int64_t>& positions) {
  std::shared_ptr<arrow::Array> sort_indices = GetSortedIndices(ctx, input);

  auto int_array = std::static_pointer_cast<arrow::Int64Array>(sort_indices);

  // key: the indice, value: the rank number
  std::unordered_map<int64_t, int64_t> row_number_count;
  BuildRankMap(int_array.get(), row_number_count,
               [](int64_t rank, int64_t /*total*/) { return rank; });

  ProcessResults(int_array->length(), row_number_count, positions,
                 window_results_);
}

const std::string PercentRank::kOpType("PercentRank");
const std::string& PercentRank::Type() const { return PercentRank::kOpType; }
void PercentRank::RunWindowFunc(ExecContext* ctx,
                                std::shared_ptr<arrow::Table> input,
                                const std::vector<int64_t>& positions) {
  std::shared_ptr<arrow::Array> sort_indices = GetSortedIndices(ctx, input);

  auto int_array = std::static_pointer_cast<arrow::Int64Array>(sort_indices);

  // key: the indice, value: the percent rank
  std::unordered_map<int64_t, double> percent_rank;
  BuildRankMap(int_array.get(), percent_rank,
               [](int64_t rank, int64_t total) { return 1.0 * rank / total; });

  ProcessResults(int_array->length(), percent_rank, positions, window_results_);
}

}  // namespace scql::engine::op