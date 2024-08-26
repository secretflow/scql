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

#include "engine/operator/unique.h"

#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor_constructor.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string Unique::kOpType("Unique");

const std::string& Unique::Type() const { return kOpType; }

void Unique::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  const auto& outputs = ctx->GetOutput(kOut);

  YACL_ENFORCE(inputs.size() == 1, "Unique input size must be 1");
  YACL_ENFORCE(inputs.size() == outputs.size(),
               "Unique input '{}' and output '{}' should have the same size",
               kIn, kOut);

  YACL_ENFORCE(util::IsTensorStatusMatched(
                   inputs[0], pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "Unique input tensor's status should be private");
  YACL_ENFORCE(util::IsTensorStatusMatched(
                   outputs[0], pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "Unique output tensor's status should be private");
}

void Unique::Execute(ExecContext* ctx) {
  const auto& input_pb = ctx->GetInput(kIn)[0];
  auto tensor = ctx->GetTensorTable()->GetTensor(input_pb.name());
  YACL_ENFORCE(tensor, "get private tensor failed, name={}", input_pb.name());

  std::shared_ptr<arrow::Array> array;
  ASSIGN_OR_THROW_ARROW_STATUS(
      array, arrow::compute::Unique(tensor->ToArrowChunkedArray()));

  auto chunked_arr = std::make_shared<arrow::ChunkedArray>(array);
  const auto& output_pb = ctx->GetOutput(kOut)[0];
  ctx->GetTensorTable()->AddTensor(output_pb.name(), TensorFrom(chunked_arr));
}

}  // namespace scql::engine::op