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

#include "engine/operator/publish.h"

#include "engine/audit/audit_log.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"
#include "engine/util/time_util.h"

namespace scql::engine::op {

const std::string Publish::kOpType("Publish");

const std::string& Publish::Type() const { return kOpType; }

void Publish::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  const auto& outputs = ctx->GetOutput(kOut);

  YACL_ENFORCE(inputs.size() == outputs.size(),
               "input size={} and output size={} not equal", inputs.size(),
               outputs.size());

  YACL_ENFORCE(util::AreTensorsStatusMatched(
                   inputs, pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "input tensors status should be private");
}

void Publish::Execute(ExecContext* ctx) {
  const auto start_time = std::chrono::system_clock::now();
  const auto& input_pbs = ctx->GetInput(kIn);
  const auto& output_pbs = ctx->GetOutput(kOut);
  int64_t num_rows = 0;
  for (int i = 0; i < input_pbs.size(); ++i) {
    const auto& input_pb = input_pbs[i];
    const auto& elem_type = input_pbs[i].elem_type();

    const auto& output_name = util::GetStringValue(output_pbs[i]);

    auto from_tensor = ctx->GetTensorTable()->GetTensor(input_pb.name());
    YACL_ENFORCE(from_tensor, "get private tensor={} failed", input_pb.name());
    if (elem_type == pb::PrimitiveDataType::TIMESTAMP &&
        !ctx->GetSession()->TimeZone().empty()) {
      from_tensor =
          util::CompensateTimeZone(from_tensor, ctx->GetSession()->TimeZone());
    }

    auto proto_result = std::make_shared<pb::Tensor>();
    SetProtoMeta(from_tensor, output_name, proto_result, elem_type);

    if (elem_type == pb::PrimitiveDataType::DATETIME) {
      // DATETIME need format to publish
      util::ConvertDateTimeAndCopyValuesToProto(from_tensor,
                                                proto_result.get());
    } else {
      util::CopyValuesToProto(from_tensor, proto_result.get());
    }
    auto dim_value = proto_result->shape().dim(0).dim_value();
    if (num_rows == 0 && dim_value != 0) {
      num_rows = dim_value;
    }
    YACL_ENFORCE_EQ(num_rows, dim_value, "num rows in result not matched {},{}",
                    num_rows, dim_value);

    ctx->GetSession()->AddPublishResult(proto_result);
  }
  audit::RecordPublishNodeDetail(*ctx, num_rows, start_time);
}

void Publish::SetProtoMeta(const std::shared_ptr<Tensor> from_tensor,
                           const std::string& name,
                           std::shared_ptr<pb::Tensor> to_proto,
                           pb::PrimitiveDataType elem_type) {
  to_proto->set_name(name);
  // set shape (rows, cols)
  auto shape = to_proto->mutable_shape();
  auto dim = shape->add_dim();
  dim->set_dim_value(from_tensor->Length());  // rows
  dim = shape->add_dim();
  dim->set_dim_value(kColumnNumInProto);

  if (elem_type == pb::PrimitiveDataType::DATETIME ||
      elem_type == pb::PrimitiveDataType::TIMESTAMP) {
    to_proto->set_elem_type(elem_type);
  } else {
    to_proto->set_elem_type(from_tensor->Type());
  }
  to_proto->set_option(pb::TensorOptions::VALUE);
  to_proto->mutable_annotation()->set_status(
      pb::TensorStatus::TENSORSTATUS_UNKNOWN);
}

}  // namespace scql::engine::op