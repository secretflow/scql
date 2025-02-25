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

#include "engine/operator/sort.h"

#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "arrow/table.h"
#include "libspu/kernel/hlo/sort.h"

#include "engine/core/tensor_constructor.h"
#include "engine/util/spu_io.h"
#include "engine/util/table_util.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string Sort::kOpType("Sort");
const std::string& Sort::Type() const { return kOpType; }

void Sort::Validate(ExecContext* ctx) {
  const auto& sort_keys = ctx->GetInput(kInKey);
  const auto& inputs = ctx->GetInput(kIn);
  const auto& outputs = ctx->GetOutput(kOut);
  YACL_ENFORCE(sort_keys.size() > 0);
  YACL_ENFORCE(inputs.size() > 0);
  YACL_ENFORCE(inputs.size() == outputs.size());

  auto input_status = util::GetTensorStatus(inputs[0]);
  YACL_ENFORCE(input_status == pb::TENSORSTATUS_PRIVATE ||
                   input_status == pb::TENSORSTATUS_SECRET,
               "operator sort only supports private or secret inputs");
  // input and output should have the same status
  YACL_ENFORCE(util::AreTensorsStatusMatched(sort_keys, input_status));
  YACL_ENFORCE(util::AreTensorsStatusMatched(inputs, input_status));
  YACL_ENFORCE(util::AreTensorsStatusMatched(outputs, input_status));
}

void Sort::Execute(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  auto input_status = util::GetTensorStatus(inputs[0]);
  if (input_status == pb::TENSORSTATUS_PRIVATE) {
    return SortInPlain(ctx);
  } else {
    return SortInSecret(ctx);
  }
}

void Sort::SortInPlain(ExecContext* ctx) {
  const auto& sort_key_pbs = ctx->GetInput(kInKey);
  const auto& in_pbs = ctx->GetInput(kIn);

  std::vector<arrow::compute::SortKey> sort_keys;

  // TODO: sort the table by the keys with different direction
  bool reverse = ctx->GetBooleanValueFromAttribute(kReverseAttr);
  arrow::compute::SortOrder order = reverse
                                        ? arrow::compute::SortOrder::Descending
                                        : arrow::compute::SortOrder::Ascending;
  sort_keys.reserve(sort_key_pbs.size());
  for (const auto& sort_key_pb : sort_key_pbs) {
    sort_keys.emplace_back(sort_key_pb.name(), order);
  }

  RepeatedPbTensor input_tensors;

  std::vector<std::string> fields;
  for (int i = 0; i < in_pbs.size(); i++) {
    // TODO: if the duplicate columns are unavoidable in some scenarios consider
    // to maitain the mapping relationship and reconstruct the table after
    // sorting
    YACL_ENFORCE(std::find(fields.begin(), fields.end(), in_pbs[i].name()) ==
                     fields.end(),
                 "duplicate field '{}'", in_pbs[i].name());
    fields.push_back(in_pbs[i].name());

    // From protobuf doc:
    // Copying to the end of this RepeatedPtrField is slowest of all; it can't
    // reliably copy-construct to the last element of this RepeatedPtrField, for
    // example (unlike std::vector).
    // We currently block this API.  The right way to add to the end is to call
    // Add() and modify the element it points to.
    // If you must add an existing value, call *Add() = value;
    *(input_tensors.Add()) = in_pbs[i];
  }

  for (int i = 0; i < sort_key_pbs.size(); i++) {
    // payload fields already contains sort key
    if (std::find(fields.begin(), fields.end(), sort_key_pbs[i].name()) !=
        fields.end()) {
      continue;
    }
    *(input_tensors.Add()) = sort_key_pbs[i];
  }

  std::shared_ptr<arrow::Table> input_table =
      util::ConstructTableFromTensors(ctx, input_tensors);

  arrow::compute::SortOptions sort_options(sort_keys);

  auto status = arrow::compute::SortIndices(input_table, sort_options);
  YACL_ENFORCE(status.ok(), "failed to sort indices");
  const std::shared_ptr<arrow::Array>& indices = status.ValueOrDie();

  auto output_status = arrow::compute::Take(input_table, indices);
  YACL_ENFORCE(output_status.ok(), "failed to take indices after sorting");
  auto output_table = output_status.ValueOrDie().table();
  YACL_ENFORCE(output_table, "failed to convert datum to table");

  const auto& out_pbs = ctx->GetOutput(kOut);
  YACL_ENFORCE(out_pbs.size() <= (int)output_table->fields().size(),
               "tensors' size after sorted({}) is less than expected({})",
               output_table->fields().size(), out_pbs.size());
  for (int i = 0; i < out_pbs.size(); i++) {
    ctx->GetTensorTable()->AddTensor(out_pbs[i].name(),
                                     TensorFrom(output_table->column(i)));
  }
}

void Sort::SortInSecret(ExecContext* ctx) {
  const auto& sort_key_pbs = ctx->GetInput(kInKey);
  const auto& in_pbs = ctx->GetInput(kIn);
  const auto& out_pbs = ctx->GetOutput(kOut);

  bool reverse = ctx->GetBooleanValueFromAttribute(kReverseAttr);

  auto* symbols = ctx->GetSession()->GetDeviceSymbols();
  std::vector<spu::Value> inputs;
  for (const auto& sort_key_pb : sort_key_pbs) {
    auto value = symbols->getVar(
        util::SpuVarNameEncoder::GetValueName(sort_key_pb.name()));
    inputs.push_back(value);
  }

  size_t sort_key_num = inputs.size();

  for (const auto& in_pb : in_pbs) {
    auto value =
        symbols->getVar(util::SpuVarNameEncoder::GetValueName(in_pb.name()));
    inputs.push_back(value);
  }

  auto* sctx = ctx->GetSession()->GetSpuContext();
  auto sort_direction = spu::kernel::hal::SortDirection::Ascending;
  if (reverse) {
    sort_direction = spu::kernel::hal::SortDirection::Descending;
  }
  // NOTE: use default performance hint for SimpleSort
  auto results = spu::kernel::hlo::SimpleSort(sctx, inputs, 0, sort_direction,
                                              sort_key_num);

  for (int i = 0; i < out_pbs.size(); ++i) {
    auto idx = sort_key_num + i;
    symbols->setVar(util::SpuVarNameEncoder::GetValueName(out_pbs[i].name()),
                    results[idx]);
  }

  // TODO: sort validity too
}

}  // namespace scql::engine::op