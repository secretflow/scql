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

#include "engine/operator/arrow_func.h"

#include "absl/strings/escaping.h"
#include "arrow/compute/api.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor_constructor.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string ArrowFunc::kOpType("ArrowFunc");

const std::string& ArrowFunc::Type() const { return kOpType; }

void ArrowFunc::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  const auto& outputs = ctx->GetOutput(kOut);

  YACL_ENFORCE(outputs.size() > 0, "ArrowFunc output size must > 0");

  YACL_ENFORCE(util::AreTensorsStatusMatched(
                   inputs, pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "ArrowFunc input tensors' status should be private");
  YACL_ENFORCE(util::AreTensorsStatusMatched(
                   outputs, pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "ArrowFunc output tensors' status should be private");
}

void ArrowFunc::Execute(ExecContext* ctx) {
  const auto& in_pb = ctx->GetInput(kIn);
  const auto& out_pb = ctx->GetOutput(kOut);
  auto func_name = ctx->GetStringValueFromAttribute(kFuncNameAttr);
  auto func_opt_type = ctx->TryGetStringValueFromAttribute(kFuncOptTypeAttr);
  std::vector<arrow::Datum> inputs;
  for (const auto& input_pb : in_pb) {
    auto t = ctx->GetTensorTable()->GetTensor(input_pb.name());
    YACL_ENFORCE(t != nullptr, "get tensor={} from tensor table failed",
                 input_pb.name());
    inputs.emplace_back(t->ToArrowChunkedArray());
  }

  arrow::Result<arrow::Datum> result;
  if (func_opt_type.has_value()) {
    std::string func_opt_decoded;
    YACL_ENFORCE(
        absl::Base64Unescape(ctx->GetStringValueFromAttribute(kFuncOptionsAttr),
                             &func_opt_decoded),
        "Base64 decode arrow func options failed");
    std::unique_ptr<arrow::compute::FunctionOptions> opt;
    ASSIGN_OR_THROW_ARROW_STATUS(
        opt, arrow::compute::FunctionOptions::Deserialize(
                 func_opt_type.value(), arrow::Buffer(func_opt_decoded)));
    result = arrow::compute::CallFunction(func_name, inputs, opt.get());
  } else {
    result = arrow::compute::CallFunction(func_name, inputs);
  }

  YACL_ENFORCE(result.ok(), "invoking arrow func '{}' caught error: {}",
               func_name, result.status().ToString());

  const auto& val = result.ValueOrDie();
  YACL_ENFORCE(val.is_chunked_array(),
               "unsupported arrow func result type: {}, func_name: {}",
               arrow::ToString(val.kind()), func_name);

  YACL_ENFORCE(out_pb.size() == 1,
               "arrow func result is chunked array, output size {} != 1",
               out_pb.size());

  const auto& arr = val.chunked_array();

  if (func_name == "strptime") {
    auto resultTensor = util::ConvertDateTimeToInt64(arr);
    ctx->GetTensorTable()->AddTensor(out_pb[0].name(), std::move(resultTensor));
  } else {
    ctx->GetTensorTable()->AddTensor(out_pb[0].name(), TensorFrom(arr));
  }
}

}  // namespace scql::engine::op