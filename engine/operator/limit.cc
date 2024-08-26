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

#include "engine/operator/limit.h"

#include "libspu/kernel/hlo/geometrical.h"

#include "engine/core/tensor_constructor.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string Limit::kOpType("Limit");

const std::string& Limit::Type() const { return kOpType; }

void Limit::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  const auto& outputs = ctx->GetOutput(kOut);

  YACL_ENFORCE(inputs.size() > 0, "Limit input size must > 0");
  YACL_ENFORCE(inputs.size() == outputs.size(),
               "Limit input {} and output {} should have the same size", kIn,
               kOut);
}

void Limit::Execute(ExecContext* ctx) {
  const auto& input_pbs = ctx->GetInput(kIn);
  const auto& output_pbs = ctx->GetOutput(kOut);
  auto offset = ctx->GetInt64ValueFromAttribute(kOffset);
  auto count = ctx->GetInt64ValueFromAttribute(kCount);
  for (int i = 0; i < input_pbs.size(); ++i) {
    const auto& input_pb = input_pbs[i];
    if (util::IsTensorStatusMatched(input_pb, pb::TENSORSTATUS_PRIVATE)) {
      auto tensor = ctx->GetTensorTable()->GetTensor(input_pb.name());
      YACL_ENFORCE(tensor, "get private tensor failed, name={}",
                   input_pb.name());
      std::shared_ptr<arrow::ChunkedArray> sliced_arr;
      if (offset < tensor->Length()) {
        sliced_arr = tensor->ToArrowChunkedArray()->Slice(offset, count);
      } else {
        sliced_arr = std::make_shared<arrow::ChunkedArray>(
            std::vector<std::shared_ptr<arrow::Array>>{},
            tensor->ToArrowChunkedArray()->type());
      }

      auto result = TensorFrom(std::move(sliced_arr));
      ctx->GetTensorTable()->AddTensor(output_pbs[i].name(), std::move(result));
    } else {
      auto symbols = ctx->GetSession()->GetDeviceSymbols();
      auto sctx = ctx->GetSession()->GetSpuContext();
      auto value = symbols->getVar(
          util::SpuVarNameEncoder::GetValueName(input_pb.name()));

      auto value_result = spu::kernel::hlo::Slice(
          sctx, value, {offset}, {std::min(offset + count, value.shape()[0])},
          {1});
      symbols->setVar(
          util::SpuVarNameEncoder::GetValueName(output_pbs[i].name()),
          value_result);

#ifdef SCQL_WITH_NULL
      auto validity = symbols->getVar(
          util::SpuVarNameEncoder::GetValidityName(input_pb.name()));

      auto validity_result = spu::kernel::hlo::Slice(
          sctx, validity, {offset},
          {std::min(offset + count, value.shape()[0])}, {1});
      symbols->setVar(
          util::SpuVarNameEncoder::GetValidityName(output_pbs[i].name()),
          validity_result);
#endif  // SCQL_WITH_NULL
    }
  }
}

}  // namespace scql::engine::op