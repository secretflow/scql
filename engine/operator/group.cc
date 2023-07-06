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
#include "engine/util/spu_io.h"
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
  std::vector<arrow::Datum> keys;
  const auto& input_pbs = ctx->GetInput(kIn);
  for (int i = 0; i < input_pbs.size(); ++i) {
    const auto& input_pb = input_pbs[i];
    auto in_t = ctx->GetTensorTable()->GetTensor(input_pb.name());
    YACL_ENFORCE(in_t, "no private tensor={}", input_pb.name());
    keys.emplace_back(
        util::ConcatenateChunkedArray(in_t->ToArrowChunkedArray()));
  }

  arrow::compute::ExecBatch key_batch;
  ASSIGN_OR_THROW_ARROW_STATUS(
      key_batch, arrow::compute::ExecBatch::Make(std::move(keys)));

  std::unique_ptr<arrow::compute::Grouper> grouper;
  ASSIGN_OR_THROW_ARROW_STATUS(
      grouper, arrow::compute::Grouper::Make(key_batch.GetTypes()));

  arrow::Datum id_batch;
  ASSIGN_OR_THROW_ARROW_STATUS(
      id_batch, grouper->Consume(arrow::compute::ExecSpan(key_batch)));

  auto chunked_arr =
      std::make_shared<arrow::ChunkedArray>(id_batch.make_array());
  ctx->GetTensorTable()->AddTensor(ctx->GetOutput(kOutId)[0].name(),
                                   std::make_shared<Tensor>(chunked_arr));

  UInt32TensorBuilder builder;
  builder.Append(grouper->num_groups());
  std::shared_ptr<Tensor> num;
  builder.Finish(&num);
  ctx->GetTensorTable()->AddTensor(ctx->GetOutput(kOutNum)[0].name(), num);
}

};  // namespace scql::engine::op