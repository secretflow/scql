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

#include "engine/operator/coalesce.h"

#include "arrow/compute/api.h"

#include "engine/core/tensor_constructor.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string Coalesce::kOpType("Coalesce");

const std::string& Coalesce::Type() const { return kOpType; }

void Coalesce::Validate(ExecContext* ctx) {
  const auto& exprs = ctx->GetInput(kExprs);
  const auto& outputs = ctx->GetOutput(kOut);

  YACL_ENFORCE(exprs.size() > 0, "Coalesce input Exprs size be greater than 0",
               exprs.size());
  YACL_ENFORCE(outputs.size() == 1, "Coalesce output size={} not equal to 1",
               outputs.size());

  YACL_ENFORCE(util::AreTensorsStatusMatched(
                   exprs, pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "Coalesce input exprs tensors' status should be private");
  YACL_ENFORCE(util::IsTensorStatusMatched(
                   outputs[0], pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "Coalesce output tensor's status should be private");
}

void Coalesce::Execute(ExecContext* ctx) {
  auto exprs = ctx->GetInputTensors(kExprs);
  std::vector<arrow::Datum> datums;
  for (const auto& expr : exprs) {
    datums.emplace_back(expr->ToArrowChunkedArray());
  }

  auto result = arrow::compute::CallFunction("coalesce", datums);
  YACL_ENFORCE(result.ok(), "caught error while invoking arrow coalesce: {}",
               result.status().ToString());

  ctx->SetOutputTensor(kOut, TensorFrom(result.ValueOrDie().chunked_array()));
}

}  // namespace scql::engine::op
