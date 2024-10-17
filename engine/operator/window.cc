// Copyright 2024 Ant Group Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//   http://www.apache.org/licenses/LICENSE-2.0

#include "engine/operator/window.h"

#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "arrow/compute/row/grouper.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/table.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor_constructor.h"
#include "engine/core/type.h"
#include "engine/util/spu_io.h"
#include "engine/util/table_util.h"
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

std::shared_ptr<arrow::ChunkedArray> RankWindowBase::VectorToChunkedArray(
    const std::vector<int64_t>& vec) {
  arrow::Int64Builder builder;
  THROW_IF_ARROW_NOT_OK(builder.Reserve(vec.size()));

  for (size_t i = 0; i < vec.size(); ++i) {
    THROW_IF_ARROW_NOT_OK(builder.Append(vec[i]));
  }

  std::shared_ptr<arrow::Array> chunk;
  THROW_IF_ARROW_NOT_OK(builder.Finish(&chunk));

  std::vector<std::shared_ptr<arrow::Array>> chunks;
  chunks.push_back(chunk);

  return std::make_shared<arrow::ChunkedArray>(chunks);
}

std::vector<std::shared_ptr<arrow::ListArray>>
RankWindowBase::GetPartitionedInputs(ExecContext* ctx,
                                     const arrow::UInt32Array* partition_cast,
                                     uint32_t partition_num) {
  // TODO: handle the special case of the partition key is empty
  std::vector<std::shared_ptr<arrow::ListArray>> partitioned_inputs;
  std::shared_ptr<arrow::ListArray> partitions;
  ASSIGN_OR_THROW_ARROW_STATUS(
      partitions,
      arrow::compute::Grouper::MakeGroupings(*partition_cast, partition_num));
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

void RankWindowBase::Execute(ExecContext* ctx) {
  auto partition_array = GetPartitionId(ctx);
  auto* partition_cast =
      dynamic_cast<const arrow::UInt32Array*>(partition_array.get());
  YACL_ENFORCE(partition_cast, "cast partition id to uint32_t failed");

  int64_t tensor_length = partition_cast->length();
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
    fields.push_back(arrow::field(input_pb.name(), in_t->ArrowType()));
  }
  std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);

  std::vector<std::shared_ptr<arrow::Array>> window_results;

  std::vector<int64_t> window_result(
      tensor_length);  // window function final result
  for (uint32_t partition_i = 0; partition_i < partition_num; partition_i++) {
    std::vector<std::string> sort_keys;
    std::vector<std::shared_ptr<arrow::Array>> slices;

    for (size_t i = 0; i < partitioned_inputs.size(); i++) {
      std::shared_ptr<arrow::ListArray> tensor_slice = partitioned_inputs[i];

      std::shared_ptr<arrow::Array> partitioned_slice =
          tensor_slice->value_slice(partition_i);
      YACL_ENFORCE(partitioned_slice->length() > 0,
                   "partitioned slice(partition id: {}) is empty", partition_i);
      slices.push_back(partitioned_slice);
    }

    std::shared_ptr<arrow::Table> slice_table;

    slice_table = arrow::Table::Make(schema, slices);
    RunWindowFunc(ctx, std::move(slice_table), positions_map[partition_i],
                  window_result);
  }

  const auto& output_pbs = ctx->GetOutput(kOut);
  ctx->GetTensorTable()->AddTensor(
      output_pbs[0].name(), TensorFrom(VectorToChunkedArray(window_result)));
  SPDLOG_INFO("{} window function done", Type());
}

// sort_indices is the position indices after sort, e.g. the result [3,0,2,1]
// means the 4th row is ranked as first, first row ranked as
// second, 3rd row is ranked 3rd, second row is ranked 4th
std::shared_ptr<arrow::Array> RankWindowBase::GetSortedIndices(
    ExecContext* ctx, std::shared_ptr<arrow::Table> input) {
  std::vector<arrow::compute::SortKey> sort_keys;

  std::vector<std::string> reverse =
      ctx->GetStringValuesFromAttribute(kReverseAttr);
  YACL_ENFORCE(reverse.size() == input->fields().size(),
               "sort key numbers are not equal to that of reverse attribute");

  for (size_t i = 0; i < input->fields().size(); i++) {
    arrow::compute::SortOrder order =
        reverse[i] == "1" ? arrow::compute::SortOrder::Descending
                          : arrow::compute::SortOrder::Ascending;
    sort_keys.push_back(
        arrow::compute::SortKey(input->fields()[i]->name(), order));
  }

  arrow::compute::SortOptions sort_options(sort_keys);
  auto status = arrow::compute::SortIndices(input, sort_options);
  YACL_ENFORCE(status.ok(), "failed to sort indices");
  return status.ValueOrDie();
}

/*
 * ctx: execution context
 * input: the input table
 * positions: the position info in the raw table of the partition
 * window_result: the final table to put the result
 */
void RowNumber::RunWindowFunc(ExecContext* ctx,
                              std::shared_ptr<arrow::Table> input,
                              const std::vector<int64_t>& positions,
                              std::vector<int64_t>& window_result) {
  std::shared_ptr<arrow::Array> sort_indices = GetSortedIndices(ctx, input);
  std::shared_ptr<arrow::ChunkedArray> chunked_array;

  auto int_array = std::static_pointer_cast<arrow::Int64Array>(sort_indices);

  // key: the indice, value: the rank number
  std::unordered_map<int64_t, int64_t> row_number_count;
  int64_t rank = 1;
  for (int64_t i = 0; i < int_array->length(); ++i) {
    // there should be no duplicate row number
    YACL_ENFORCE(
        row_number_count.find(int_array->Value(i)) == row_number_count.end(),
        "duplicate row number");
    row_number_count[int_array->Value(i)] = rank;  // the rank of ith row
    rank++;
  }

  YACL_ENFORCE(int_array->length() == (int64_t)positions.size(),
               "position vector size not equal to that of input value");
  for (int64_t i = 0; i < int_array->length(); ++i) {
    YACL_ENFORCE(row_number_count.find(i) != row_number_count.end(),
                 "failed to find the indice of position {}", i);
    YACL_ENFORCE(
        positions[i] >= 0 && positions[i] < (int64_t)window_result.size(),
        "position {} is out of range [0, {}]", positions[i],
        window_result.size());
    // position[i] is the index of current row in origin table,
    // row_number_count[i] is the rank number of current row
    window_result[positions[i]] = row_number_count[i];
  }
}

}  // namespace scql::engine::op