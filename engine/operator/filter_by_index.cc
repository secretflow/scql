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

#include "engine/operator/filter_by_index.h"

#include "arrow/compute/exec.h"
#include "arrow/datum.h"
#include "arrow/result.h"

#include "engine/core/tensor_constructor.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string FilterByIndex::kOpType("FilterByIndex");

const std::string& FilterByIndex::Type() const { return kOpType; }

void FilterByIndex::Validate(ExecContext* ctx) {
  // check visibility of i/o
  const auto& indice = ctx->GetInput(kInRowsIndexFilter)[0];
  YACL_ENFORCE(util::IsTensorStatusMatched(
                   indice, pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "{} tensor {} status should be private, but got {}",
               kInRowsIndexFilter, indice.name(),
               pb::TensorStatus_Name(util::GetTensorStatus(indice)));

  const auto& data = ctx->GetInput(kInData);
  YACL_ENFORCE(util::AreTensorsStatusMatched(
      data, pb::TensorStatus::TENSORSTATUS_PRIVATE));

  const auto& out = ctx->GetOutput(kOut);
  YACL_ENFORCE(util::AreTensorsStatusMatched(
      out, pb::TensorStatus::TENSORSTATUS_PRIVATE));

  YACL_ENFORCE(data.size() == out.size(),
               "input {} and output {} should have the same size", kInData,
               kOut);
}

void FilterByIndex::Execute(ExecContext* ctx) {
  auto indice = ctx->GetInputTensor(kInRowsIndexFilter);
  auto data_tensors = ctx->GetInputTensors(kInData);

  std::vector<std::shared_ptr<Tensor>> results;
  results.reserve(data_tensors.size());
  for (const auto& data_tensor : data_tensors) {
    results.push_back(Take(*data_tensor, *indice));
  }

  ctx->SetOutputTensors(kOut, results);
}

TensorPtr FilterByIndex::Take(const Tensor& value, const Tensor& indice) {
  arrow::Result<arrow::Datum> result;
  // delegate to apache arrow's take function
  result = arrow::compute::CallFunction(
      "take", {value.ToArrowChunkedArray(), indice.ToArrowChunkedArray()});

  YACL_ENFORCE(result.ok(),
               "caught error while invoking arrow take function: {}",
               result.status().ToString());

  return TensorFrom(result.ValueOrDie().chunked_array());
}

};  // namespace scql::engine::op