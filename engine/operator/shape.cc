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

#include "engine/operator/shape.h"

#include "engine/core/primitive_builder.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string Shape::kOpType("Shape");

const std::string& Shape::Type() const { return kOpType; }

void Shape::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  const auto& outputs = ctx->GetOutput(kOut);

  YACL_ENFORCE(inputs.size() > 0, "Shape input size must > 0");
  YACL_ENFORCE(inputs.size() == outputs.size(),
               "Shape input {} and output {} should have the same size", kIn,
               kOut);

  YACL_ENFORCE(util::AreTensorsStatusMatched(
                   outputs, pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "Shape output tensors' status should all be private");
}

void Shape::Execute(ExecContext* ctx) {
  const auto& input_pbs = ctx->GetInput(kIn);
  const auto& output_pbs = ctx->GetOutput(kOut);
  for (int i = 0; i < input_pbs.size(); ++i) {
    const auto& input_pb = input_pbs[i];
    std::pair<int64_t /* row count */, int64_t /* column count */> shapes;
    shapes.second = 1;  // tensor only contains one column.
    if (util::IsTensorStatusMatched(input_pb, pb::TENSORSTATUS_PRIVATE)) {
      auto tensor = ctx->GetTensorTable()->GetTensor(input_pb.name());
      YACL_ENFORCE(tensor, "get private tensor failed, name={}",
                   input_pb.name());
      shapes.first = tensor->Length();
    } else if (util::IsTensorStatusMatched(input_pb, pb::TENSORSTATUS_SECRET)) {
      auto device_symbols = ctx->GetSession()->GetDeviceSymbols();
      auto value = device_symbols->getVar(
          util::SpuVarNameEncoder::GetValueName(input_pb.name()));
      shapes.first =
          value.shape().size() > 0 ? value.shape()[0] : value.numel();
    } else {
      YACL_THROW("not support input status={}",
                 pb::TensorStatus_Name(util::GetTensorStatus(input_pb)));
    }

    auto result = CreateShapeTensor(ctx, shapes);

    ctx->GetTensorTable()->AddTensor(output_pbs[i].name(), std::move(result));
  }
}

TensorPtr Shape::CreateShapeTensor(ExecContext* ctx,
                                   const std::pair<int64_t, int64_t>& shapes) {
  auto logger = ctx->GetActiveLogger();
  int64_t axis = kAxisDefault;
  try {
    axis = ctx->GetInt64ValueFromAttribute(kAxis);
  } catch (const ::yacl::EnforceNotMet& e) {
    SPDLOG_LOGGER_WARN(
        logger,
        "not set attribute axis, default get (row count, 1), exception={}",
        e.what());
  }

  Int64TensorBuilder builder;
  if (axis == kAxisRow) {
    builder.Append(shapes.first);
  } else if (axis == kAxisColumn) {
    builder.Append(shapes.second);
  } else {
    builder.Append(shapes.first);
    builder.Append(shapes.second);
  }

  TensorPtr result;
  builder.Finish(&result);

  return result;
}

}  // namespace scql::engine::op