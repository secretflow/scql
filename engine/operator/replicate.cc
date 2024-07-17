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

#include "engine/operator/replicate.h"

#include "arrow/array/array_base.h"
#include "arrow/builder.h"
#include "msgpack.hpp"

#include "engine/core/arrow_helper.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string Replicate::kOpType("Replicate");

const std::string& Replicate::Type() const { return kOpType; }

void Replicate::Validate(ExecContext* ctx) {
  const auto& left = ctx->GetInput(kInLeft);
  const auto& right = ctx->GetInput(kInRight);

  YACL_ENFORCE(left.size() > 0, "Replicate input Left size be greater than 0",
               left.size());
  YACL_ENFORCE(right.size() > 0, "Replicate input Right size be greater than 0",
               right.size());

  YACL_ENFORCE(util::AreTensorsStatusMatched(
                   left, pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "Replicate input Left tensors' status should be private");
  YACL_ENFORCE(util::AreTensorsStatusMatched(
                   right, pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "Replicate input Right tensors' status should be private");
}

void Replicate::Execute(ExecContext* ctx) {
  std::vector<std::string> input_party_codes =
      ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);
  YACL_ENFORCE(input_party_codes.size() == 2,
               "input party codes size %d not equal to 2",
               input_party_codes.size());

  bool is_left = ctx->GetSession()->SelfPartyCode() == input_party_codes[0];
  const auto& input_proto = ctx->GetInput(is_left ? kInLeft : kInRight);
  const auto& output_proto = ctx->GetOutput(is_left ? kOutLeft : kOutRight);

  std::vector<TensorPtr> tensors;
  for (int i = 0; i < input_proto.size(); ++i) {
    auto ts = ctx->GetTensorTable()->GetTensor(input_proto[i].name());
    YACL_ENFORCE(ts != nullptr, "get tensor={} from tensor table failed",
                 input_proto[i].name());
    tensors.push_back(ts);
  }

  // communicate tensor length
  YACL_ENFORCE(tensors.size() > 0);
  int64_t tensor_length = tensors[0]->Length();
  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, tensor_length);

  std::string tag = "ReplicateLengthTag";
  auto psi_link = ctx->GetSession()->GetLink();
  if (psi_link->WorldSize() > 2) {
    psi_link = psi_link->SubWorld(tag, input_party_codes);
  }
  auto length_bufs = yacl::link::AllGather(
      psi_link, yacl::ByteContainerView(sbuf.data(), sbuf.size()), tag);
  auto& peer_length_buf = length_bufs[psi_link->NextRank()];
  msgpack::object_handle oh = msgpack::unpack(
      static_cast<char*>(peer_length_buf.data()), peer_length_buf.size());
  auto peer_length = oh.get().as<int64_t>();

  // replicate tensors
  for (size_t i = 0; i < tensors.size(); ++i) {
    auto chunked_array = tensors[i]->ToArrowChunkedArray();
    arrow::ArrayVector arrays;
    if (is_left) {
      for (int64_t j = 0; j < peer_length; ++j) {
        for (int k = 0; k < chunked_array->num_chunks(); ++k) {
          std::shared_ptr<arrow::Array> array =
              chunked_array->chunk(k)->View(chunked_array->type()).ValueOrDie();
          arrays.push_back(std::move(array));
        }
      }
    } else {
      std::unique_ptr<arrow::ArrayBuilder> builder;
      ASSIGN_OR_THROW_ARROW_STATUS(builder,
                                   arrow::MakeBuilder(chunked_array->type()));
      for (int64_t j = 0; j < chunked_array->length(); ++j) {
        // TODO: split to multiple chunks
        THROW_IF_ARROW_NOT_OK(builder->AppendScalar(
            *(chunked_array->GetScalar(j).ValueOrDie()), peer_length));
      }
      std::shared_ptr<arrow::Array> array;
      THROW_IF_ARROW_NOT_OK(builder->Finish(&array));
      arrays.push_back(std::move(array));
    }

    auto result_chunked = arrow::ChunkedArray::Make(arrays).ValueOrDie();
    auto t = std::make_shared<Tensor>(result_chunked);
    ctx->GetTensorTable()->AddTensor(output_proto[i].name(), std::move(t));
  }
}

}  // namespace scql::engine::op