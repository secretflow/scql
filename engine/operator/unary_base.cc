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

#include "engine/operator/unary_base.h"

#include "engine/util/tensor_util.h"

namespace scql::engine::op {
void UnaryBase::Validate(ExecContext* ctx) {
  const auto& in_pb = ctx->GetInput(kIn);
  const auto& out_pb = ctx->GetOutput(kOut);
  YACL_ENFORCE(in_pb.size() == 1, "unary operators only support one input");
  YACL_ENFORCE(out_pb.size() == 1, "unary operators only support one output");
  YACL_ENFORCE(
      util::GetTensorStatus(in_pb[0]) == util::GetTensorStatus(out_pb[0]),
      "input and output should have same status");
}

void UnaryBase::Execute(ExecContext* ctx) {
  if (ctx->GetOutputStatus(kOut) == pb::TENSORSTATUS_PRIVATE) {
    ExecuteInPlain(ctx);
  } else {
    ExecuteInSecret(ctx);
  }
}

void UnaryBase::ExecuteInPlain(ExecContext* ctx) {
  SPDLOG_INFO("inside ExecuteInPlain");
  auto in_ptr = ctx->GetInputTensor(kIn);
  YACL_ENFORCE(in_ptr != nullptr);

  auto result = ComputeInPlain(*in_ptr);
  ctx->SetOutputTensor(kOut, std::move(result));
}

void UnaryBase::ExecuteInSecret(ExecContext* ctx) {
  auto in_value = ctx->GetInputValue(kIn);
  auto result_value =
      ComputeOnSpu(ctx->GetSession()->GetSpuContext(), in_value);
  ctx->SetOutputValue(kOut, result_value);
}
}  // namespace scql::engine::op
