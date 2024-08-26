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

#include "engine/operator/group.h"

#include "arrow/compute/exec.h"
#include "arrow/compute/row/grouper.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/primitive_builder.h"
#include "engine/core/tensor_constructor.h"
#include "engine/util/spu_io.h"
#include "engine/util/table_util.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string Group::kOpType("Group");
const std::string& Group::Type() const { return kOpType; }

void Group::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  YACL_ENFORCE(inputs.size() > 0, "input size cannot be 0");
  const auto& out_id = ctx->GetOutput(kOutId);
  YACL_ENFORCE(out_id.size() == 1, "output group id's size={} not equal to 1",
               out_id.size());
  const auto& out_num = ctx->GetOutput(kOutNum);
  YACL_ENFORCE(out_num.size() == 1, "output group num's size={} not equal to 1",
               out_num.size());

  YACL_ENFORCE(util::AreTensorsStatusMatched(inputs, pb::TENSORSTATUS_PRIVATE),
               "inputs' status are not all private");
  YACL_ENFORCE(util::IsTensorStatusMatched(out_id[0], pb::TENSORSTATUS_PRIVATE),
               "output's status is not private");
  YACL_ENFORCE(
      util::IsTensorStatusMatched(out_num[0], pb::TENSORSTATUS_PRIVATE),
      "output's status is not private");
}

void Group::Execute(ExecContext* ctx) {
  const auto& input_pbs = ctx->GetInput(kIn);
  auto table = util::ConstructTableFromTensors(ctx, input_pbs);
  YACL_ENFORCE(table, "construct table failed");

  std::vector<arrow::TypeHolder> key_types;
  for (auto field : table->schema()->fields()) {
    key_types.push_back(field->type());
  }
  std::unique_ptr<arrow::compute::Grouper> grouper;
  ASSIGN_OR_THROW_ARROW_STATUS(grouper,
                               arrow::compute::Grouper::Make(key_types));

  auto reader = arrow::TableBatchReader(table);
  // NOTE: limit chunk size to 1000W to avoid overflow in Grouper::Consume
  // TODO: make chunk size configurable
  reader.set_chunksize(1000 * 10000);
  arrow::ArrayVector id_chunks;
  while (true) {
    std::shared_ptr<arrow::RecordBatch> batch;
    ASSIGN_OR_THROW_ARROW_STATUS(batch, reader.Next());
    if (batch == nullptr) {
      break;
    }
    arrow::Datum id_batch;
    ASSIGN_OR_THROW_ARROW_STATUS(id_batch,
                                 grouper->Consume(arrow::compute::ExecSpan(
                                     arrow::compute::ExecBatch(*batch))));
    id_chunks.push_back(id_batch.make_array());
  }
  auto chunked_arr =
      std::make_shared<arrow::ChunkedArray>(id_chunks, arrow::uint32());
  ctx->GetTensorTable()->AddTensor(ctx->GetOutput(kOutId)[0].name(),
                                   TensorFrom(chunked_arr));

  UInt32TensorBuilder builder;
  builder.Append(grouper->num_groups());
  std::shared_ptr<Tensor> num;
  builder.Finish(&num);
  ctx->GetTensorTable()->AddTensor(ctx->GetOutput(kOutNum)[0].name(), num);
}

};  // namespace scql::engine::op