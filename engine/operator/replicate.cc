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

#include "arrow/array/builder_base.h"
#include "arrow/scalar.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor_constructor.h"
#include "engine/util/context_util.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {
const std::string Replicate::kOpType("Replicate");

const std::string& Replicate::Type() const { return kOpType; }

void Replicate::Validate(ExecContext* ctx) {
  const auto& codes = ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);
  YACL_ENFORCE(codes.size() == 2, "Replicate only support 2 parties");
  std::string my_party = ctx->GetSession()->SelfPartyCode();
  if (my_party == codes[0]) {
    const auto& left_input = ctx->GetInput(kLeft);
    int64_t length = -1;
    for (const auto& left : left_input) {
      YACL_ENFORCE(util::IsTensorStatusMatched(left, pb::TENSORSTATUS_PRIVATE));
      TensorPtr left_ptr = util::GetPrivateOrPublicTensor(ctx, left);
      YACL_ENFORCE(left_ptr, "tensor {} not found", left.name());
      if (length == -1) {
        length = left_ptr->Length();
      }

      YACL_ENFORCE(left_ptr->Length() == length,
                   "different length in left input tensors");
    }
  }

  if (my_party == codes[1]) {
    const auto& right_input = ctx->GetInput(kRight);
    int64_t length = -1;
    for (const auto& right : right_input) {
      YACL_ENFORCE(
          util::IsTensorStatusMatched(right, pb::TENSORSTATUS_PRIVATE));
      TensorPtr right_ptr = util::GetPrivateOrPublicTensor(ctx, right);
      YACL_ENFORCE(right_ptr, "tensor {} not found", right.name());
      if (length == -1) {
        length = right_ptr->Length();
      }

      YACL_ENFORCE(right_ptr->Length() == length,
                   "different length in right input tensors");
    }
  }
}

std::shared_ptr<arrow::ChunkedArray> Replicate::ExtendArray(
    const std::shared_ptr<arrow::ChunkedArray>& array, int64_t extend_size,
    bool interleaving) {
  if (!array) {
    return nullptr;
  }
  if (extend_size == 0 || array->num_chunks() == 0) {
    std::shared_ptr<arrow::DataType> type = array->type();
    auto result = arrow::ChunkedArray::Make({}, type);
    YACL_ENFORCE(result.ok(), "failed to make empty chunked array, error: {}",
                 result.status().ToString());

    return result.ValueOrDie();
  }
  std::vector<std::shared_ptr<arrow::Array>> new_chunks;
  if (interleaving) {
    for (int64_t i = 0; i < extend_size; i++) {
      for (const auto& chunk : array->chunks()) {
        new_chunks.push_back(chunk);
      }
    }
  } else {
    std::unique_ptr<arrow::ArrayBuilder> builder;
    THROW_IF_ARROW_NOT_OK(arrow::MakeBuilder(arrow::default_memory_pool(),
                                             array->type(), &builder));
    for (int i = 0; i < array->num_chunks(); i++) {
      std::shared_ptr<arrow::Array> chunk = array->chunk(i);
      for (int j = 0; j < chunk->length(); j++) {
        for (int k = 0; k < extend_size; k++) {
          auto value = chunk->GetScalar(j);
          YACL_ENFORCE(value.ok(), "failed to get scalar, error: {}",
                       value.status().ToString());
          THROW_IF_ARROW_NOT_OK(builder->AppendScalar(*value.ValueOrDie()));
        }
      }
    }
    std::shared_ptr<arrow::Array> result;
    THROW_IF_ARROW_NOT_OK(builder->Finish(&result));
    new_chunks.push_back(result);
  }

  return std::make_shared<arrow::ChunkedArray>(new_chunks);
}

void Replicate::BuildReplicate(ExecContext* ctx, const RepeatedPbTensor& inputs,
                               size_t dup_rows, const RepeatedPbTensor& outputs,
                               bool interleaving) {
  YACL_ENFORCE(inputs.size() == outputs.size(),
               "input size {} != output size {}", inputs.size(),
               outputs.size());
  int64_t input_tensor_length = -1;
  for (int i = 0; i < inputs.size(); i++) {
    const auto& input_tensor = inputs[i];
    YACL_ENFORCE(
        util::IsTensorStatusMatched(input_tensor, pb::TENSORSTATUS_PRIVATE),
        "replicate tensors must be private");
    TensorPtr input_ptr = util::GetPrivateOrPublicTensor(ctx, input_tensor);
    YACL_ENFORCE(input_ptr, "tensor {} not found", input_tensor.name());
    if (input_tensor_length == -1) {
      input_tensor_length = input_ptr->Length();
    }

    YACL_ENFORCE(input_ptr->Length() == input_tensor_length,
                 "input tensors length must be equal");
    auto chunked_arr = input_ptr->ToArrowChunkedArray();
    YACL_ENFORCE(chunked_arr, "input tensor {} is not valid",
                 input_tensor.name());
    auto new_array = ExtendArray(chunked_arr, dup_rows, interleaving);
    YACL_ENFORCE(new_array, "failed to extend array {}", input_tensor.name());
    ctx->GetTensorTable()->AddTensor(outputs[i].name(), TensorFrom(new_array));
  }
}

void Replicate::Execute(ExecContext* ctx) {
  const auto& left = ctx->GetInput(kLeft);
  const auto& right = ctx->GetInput(kRight);
  const auto& left_out = ctx->GetOutput(kLeftOut);
  const auto& right_out = ctx->GetOutput(kRightOut);

  const auto& codes = ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);
  std::string my_party = ctx->GetSession()->SelfPartyCode();
  auto lctx = ctx->GetSession()->GetLink();
  std::string tag = ctx->GetNodeName() + "-Replicate";
  lctx = lctx->SubWorld(tag, codes);

  if (my_party == codes[0]) {
    if (my_party == codes[1]) {
      TensorPtr right_input = util::GetPrivateOrPublicTensor(ctx, right[0]);
      YACL_ENFORCE(right_input, "tensor {} not found", right[0].name());
      BuildReplicate(ctx, left, right_input->Length(), left_out, true);
    } else {
      TensorPtr left_input = util::GetPrivateOrPublicTensor(ctx, left[0]);
      YACL_ENFORCE(left_input, "tensor {} not found", left[0].name());
      size_t left_length = left_input->Length();

      auto length_bufs = yacl::link::AllGather(
          lctx, yacl::ByteContainerView(&left_length, sizeof(left_length)),
          tag);
      YACL_ENFORCE(
          lctx->NextRank() >= 0 && lctx->NextRank() < length_bufs.size(),
          "invalid rank({}) for buf size({})", lctx->NextRank(),
          length_bufs.size());
      const size_t* right_length = length_bufs[lctx->NextRank()].data<size_t>();
      YACL_ENFORCE(right_length);
      BuildReplicate(ctx, left, *right_length, left_out, true);
    }
  }
  if (my_party == codes[1]) {
    if (my_party == codes[0]) {
      TensorPtr left_input = util::GetPrivateOrPublicTensor(ctx, left[0]);
      YACL_ENFORCE(left_input, "tensor {} not found", left[0].name());
      BuildReplicate(ctx, right, left_input->Length(), right_out, false);
    } else {
      TensorPtr right_input = util::GetPrivateOrPublicTensor(ctx, right[0]);
      YACL_ENFORCE(right_input, "tensor {} not found", right[0].name());
      size_t right_length = right_input->Length();
      auto length_bufs = yacl::link::AllGather(
          lctx, yacl::ByteContainerView(&right_length, sizeof(right_length)),
          tag);

      const size_t* left_length;
      YACL_ENFORCE(
          lctx->NextRank() >= 0 && lctx->NextRank() < length_bufs.size(),
          "invalid rank({}) for buf size({})", lctx->NextRank(),
          length_bufs.size());
      left_length = length_bufs[lctx->NextRank()].data<size_t>();
      YACL_ENFORCE(left_length);

      BuildReplicate(ctx, right, *left_length, right_out, false);
    }
  }
}
}  // namespace scql::engine::op