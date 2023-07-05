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

#include "engine/operator/make_private.h"

#include "engine/core/string_tensor_builder.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string MakePrivate::kOpType = "MakePrivate";

const std::string& MakePrivate::Type() const { return kOpType; }

void MakePrivate::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  const auto& outputs = ctx->GetOutput(kOut);

  YACL_ENFORCE(inputs.size() == outputs.size(),
               "MakePrivate input {} and output {} should have the same size",
               kIn, kOut);
  // TODO(shunde.csd): support TENSORSTATUS_CIPHER inputs
  YACL_ENFORCE(
      util::AreTensorsStatusEqualAndOneOf(
          inputs, {pb::TENSORSTATUS_SECRET, pb::TENSORSTATUS_PUBLIC}),
      "MakePrivate input tensor's status should all be secret or public");

  YACL_ENFORCE(util::AreTensorsStatusMatched(outputs, pb::TENSORSTATUS_PRIVATE),
               "MakePrivate output tensor's status should all be private");

  std::vector<std::string> reveal_to_parties =
      ctx->GetStringValuesFromAttribute(kRevealToAttr);
  YACL_ENFORCE(reveal_to_parties.size() == 1,
               "MakePrivate operator attribute {} should have exactly 1 "
               "element, but got {}",
               kRevealToAttr, reveal_to_parties.size());
}

void MakePrivate::Execute(ExecContext* ctx) {
  auto reveal_to_party = ctx->GetStringValueFromAttribute(kRevealToAttr);
  auto reveal_to_rank = ctx->GetSession()->GetPartyRank(reveal_to_party);
  YACL_ENFORCE(reveal_to_rank != -1, "unknown rank for party {}",
               reveal_to_party);

  bool reveal_to_me = ctx->GetSession()->SelfPartyCode() == reveal_to_party;

  const auto& input_pbs = ctx->GetInput(kIn);
  const auto& output_pbs = ctx->GetOutput(kOut);
  util::SpuOutfeedHelper io(ctx->GetSession()->GetSpuContext(),
                            ctx->GetSession()->GetDeviceSymbols());
  for (int i = 0; i < input_pbs.size(); ++i) {
    TensorPtr t;
    if (util::IsTensorStatusMatched(input_pbs[i], pb::TENSORSTATUS_SECRET)) {
      t = io.RevealTo(input_pbs[i].name(), reveal_to_rank);
      if (reveal_to_me &&
          input_pbs[i].elem_type() == pb::PrimitiveDataType::STRING) {
        YACL_ENFORCE(t);
        t = ctx->GetSession()->HashToString(*t);
      }
    } else {
      auto spu_io =
          util::SpuOutfeedHelper(ctx->GetSession()->GetSpuContext(),
                                 ctx->GetSession()->GetDeviceSymbols());
      t = spu_io.DumpPublic(input_pbs[i].name());
      YACL_ENFORCE(t);
      if (input_pbs[i].elem_type() == pb::PrimitiveDataType::STRING) {
        t = RevealPublicString(ctx, t, reveal_to_me, reveal_to_rank);
      }
    }

    if (reveal_to_me) {
      YACL_ENFORCE(t);
      ctx->GetTensorTable()->AddTensor(output_pbs[i].name(), std::move(t));
    }
  }
}

TensorPtr MakePrivate::RevealPublicString(ExecContext* ctx, TensorPtr t,
                                          bool reveal_to_me,
                                          size_t reveal_to_rank) {
  // TODO(jingshi): optimization: remove "__null__" in string_t to reduce
  // network data interaction
  auto string_t = ctx->GetSession()->HashToString(*t);
  auto proto_t = std::make_shared<pb::Tensor>();
  util::CopyValuesToProto(string_t, proto_t.get());
  std::string content;
  YACL_ENFORCE(proto_t->SerializeToString(&content));
  auto contents = yacl::link::Gather(ctx->GetSession()->GetLink(),
                                     yacl::ByteContainerView(content),
                                     reveal_to_rank, "string_make_private");

  if (!reveal_to_me) {
    return nullptr;
  }

  std::vector<pb::Tensor> proto_ts;
  for (const auto& content : contents) {
    pb::Tensor proto_t;
    YACL_ENFORCE(proto_t.ParseFromArray(content.data(), content.size()));
    proto_ts.push_back(proto_t);
  }
  YACL_ENFORCE(proto_ts.size() > 0);
  int row_num = proto_ts[0].string_data_size();
  for (size_t rank = 1; rank < proto_ts.size(); ++rank) {
    YACL_ENFORCE(proto_ts[rank].string_data_size() == row_num,
                 "rank#{} row_num={} not equal to rank#0 row_num={}", rank,
                 proto_ts[rank].string_data_size(), row_num);
  }

  StringTensorBuilder builder;
  const std::string kStringPlaceHolder = "__null__";
  for (int i = 0; i < row_num; ++i) {
    std::string tmp = proto_ts[0].string_data(i);
    for (size_t j = 1; j < proto_ts.size(); ++j) {
      if (proto_ts[j].string_data(i) != kStringPlaceHolder) {
        tmp = proto_ts[j].string_data(i);
      }
    }
    builder.Append(tmp);
  }
  TensorPtr result;
  builder.Finish(&result);

  return result;
}

}  // namespace scql::engine::op