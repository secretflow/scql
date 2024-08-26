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

#include "engine/operator/if_null.h"

#include "arrow/compute/api.h"

#include "engine/core/tensor_constructor.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string IfNull::kOpType("IfNull");

const std::string& IfNull::Type() const { return kOpType; }

void IfNull::Validate(ExecContext* ctx) {
  const auto& expr = ctx->GetInput(kExpr);
  const auto& alt_value = ctx->GetInput(kAltValue);
  const auto& outputs = ctx->GetOutput(kOut);

  YACL_ENFORCE(expr.size() == 1, "IfNull input Expr size={} not equal to 1",
               expr.size());
  YACL_ENFORCE(alt_value.size() == 1,
               "IfNull input altValue size={} not equal to 1",
               alt_value.size());
  YACL_ENFORCE(outputs.size() == 1, "IfNull output size={} not equal to 1",
               outputs.size());

  YACL_ENFORCE(util::IsTensorStatusMatched(
                   expr[0], pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "IfNull input expr tensor's status should be private");
  YACL_ENFORCE(util::IsTensorStatusMatched(
                   alt_value[0], pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "IfNull input altValue tensor's status should be private");
  YACL_ENFORCE(util::IsTensorStatusMatched(
                   outputs[0], pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "IfNull output tensor's status should be private");
}

void IfNull::Execute(ExecContext* ctx) {
  const auto& expr_pb = ctx->GetInput(kExpr)[0];
  const auto& alt_pb = ctx->GetInput(kAltValue)[0];
  const auto& output_pb = ctx->GetOutput(kOut)[0];
  auto expr = ctx->GetTensorTable()->GetTensor(expr_pb.name());
  YACL_ENFORCE(expr != nullptr, "get tensor={} from tensor table failed",
               expr_pb.name());
  auto alt_value = ctx->GetTensorTable()->GetTensor(alt_pb.name());
  YACL_ENFORCE(alt_value != nullptr);

  auto result = arrow::compute::CallFunction(
      "coalesce",
      {expr->ToArrowChunkedArray(), alt_value->ToArrowChunkedArray()});
  YACL_ENFORCE(result.ok(), "caught error while invoking arrow coalesce: {}",
               result.status().ToString());

  auto t = TensorFrom(result.ValueOrDie().chunked_array());
  ctx->GetTensorTable()->AddTensor(output_pb.name(), std::move(t));
}

}  // namespace scql::engine::op