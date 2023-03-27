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

#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string Publish::kOpType("Publish");

const std::string& Publish::Type() const { return kOpType; }

void Publish::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  const auto& outputs = ctx->GetOutput(kOut);

  YACL_ENFORCE(inputs.size() == outputs.size(),
               "input size={} and output size={} not equal", inputs.size(),
               outputs.size());
  for (const auto& input : inputs) {
    YACL_ENFORCE(util::IsTensorStatusMatched(
                     input, pb::TensorStatus::TENSORSTATUS_PRIVATE) ||
                     util::IsTensorStatusMatched(
                         input, pb::TensorStatus::TENSORSTATUS_PUBLIC),
                 "input tensor status should be private or public");
  }
}

void Publish::Execute(ExecContext* ctx) {
  const auto& input_pbs = ctx->GetInput(kIn);
  const auto& output_pbs = ctx->GetOutput(kOut);
  for (int i = 0; i < input_pbs.size(); ++i) {
    const auto& input_pb = input_pbs[i];
    const auto& output_name = output_pbs[i].name();

    auto from_tensor = GetPrivateOrPublicTensor(ctx, input_pb);
    YACL_ENFORCE(from_tensor, "get private or public tensor failed");

    auto proto_result = std::make_shared<pb::Tensor>();
    SetProtoMeta(from_tensor, output_name, proto_result);

    util::CopyValuesToProto(from_tensor, proto_result.get());

    ctx->GetSession()->AddPublishResult(proto_result);
  }
}

TensorPtr Publish::GetPrivateOrPublicTensor(ExecContext* ctx,
                                            const pb::Tensor& input_pb) {
  TensorPtr ret;
  if (util::IsTensorStatusMatched(input_pb, pb::TENSORSTATUS_PRIVATE)) {
    ret = ctx->GetTensorTable()->GetTensor(input_pb.name());
  } else if (util::IsTensorStatusMatched(input_pb, pb::TENSORSTATUS_PUBLIC)) {
    // read public tensor from spu device symbol table
    auto spu_io = util::SpuOutfeedHelper(ctx->GetSession()->GetSpuHalContext(),
                                         ctx->GetSession()->GetDeviceSymbols());
    ret = spu_io.DumpPublic(input_pb.name());

    if (input_pb.elem_type() == pb::PrimitiveDataType::STRING) {
      ret = ctx->GetSession()->HashToString(*ret);
    }
  } else {
    YACL_THROW("status (\"{}\") must be either private or public",
               input_pb.name());
  }
  return ret;
}

void Publish::SetProtoMeta(const std::shared_ptr<Tensor> from_tensor,
                           const std::string& name,
                           std::shared_ptr<pb::Tensor> to_proto) {
  to_proto->set_name(name);
  // set shape (rows, cols)
  auto shape = to_proto->mutable_shape();
  auto dim = shape->add_dim();
  dim->set_dim_value(from_tensor->Length());  // rows
  dim = shape->add_dim();
  dim->set_dim_value(kColumnNumInProto);

  to_proto->set_elem_type(from_tensor->Type());
  to_proto->set_option(pb::TensorOptions::VALUE);
  to_proto->mutable_annotation()->set_status(
      pb::TensorStatus::TENSORSTATUS_UNKNOWN);
}

}  // namespace scql::engine::op