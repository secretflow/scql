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

#include "engine/operator/is_null.h"

#include "arrow/compute/api.h"

#include "engine/core/tensor_constructor.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string IsNull::kOpType("IsNull");

const std::string& IsNull::Type() const { return kOpType; }

void IsNull::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  const auto& outputs = ctx->GetOutput(kOut);

  YACL_ENFORCE(inputs.size() == 1, "IsNull input size={} not equal to 1",
               inputs.size());
  YACL_ENFORCE(outputs.size() == 1, "IsNull output size={} not equal to 1",
               outputs.size());

  YACL_ENFORCE(util::IsTensorStatusMatched(
                   inputs[0], pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "IsNull input tensor's status should be private");
  YACL_ENFORCE(util::IsTensorStatusMatched(
                   outputs[0], pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "IsNull output tensor's status should be private");
}

void IsNull::Execute(ExecContext* ctx) {
  auto tensor = ctx->GetInputTensor(kIn);

  arrow::compute::NullOptions options;
  auto result = arrow::compute::IsNull(tensor->ToArrowChunkedArray(), options);
  YACL_ENFORCE(result.ok(), "caught error while invoking arrow IsNull: {}",
               result.status().ToString());

  ctx->SetOutputTensor(kOut, TensorFrom(result.ValueOrDie().chunked_array()));
}

}  // namespace scql::engine::op