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

#include "engine/operator/group_he_sum.h"

#include "arrow/array.h"
#include "arrow/compute/exec.h"
#include "arrow/scalar.h"
#include "gflags/gflags.h"
#include "heu/library/numpy/numpy.h"
#include "heu/library/phe/encoding/plain_encoder.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/primitive_builder.h"
#include "engine/core/type.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

// clang-format off
DEFINE_bool(
    enable_he_schema_type_ou, false,
    "whether to use OU to speed up HeSum, use ZPaillier by default for security,"
    "see: https://www.secretflow.org.cn/docs/heu/latest/zh-Hans/getting_started/algo_choice#ou-paillier");
// clang-format on
namespace scql::engine::op {

using heu::lib::numpy::CMatrix;
using heu::lib::numpy::DenseMatrix;
using heu::lib::numpy::DestinationHeKit;
using heu::lib::numpy::HeKit;
using heu::lib::numpy::PMatrix;
using heu::lib::phe::Ciphertext;
using heu::lib::phe::PlainEncoder;
using heu::lib::phe::Plaintext;

const std::string GroupHeSum::kOpType("GroupHeSum");

const std::string& GroupHeSum::Type() const { return kOpType; }

namespace {

heu::lib::phe::SchemaType GetHeSchemaType() {
  if (FLAGS_enable_he_schema_type_ou) {
    return heu::lib::phe::SchemaType::OU;
  }
  return heu::lib::phe::SchemaType::ZPaillier;
}

#ifndef TENSOR_TO_PMATRIX_TYPE_IMPL
#define TENSOR_TO_PMATRIX_TYPE_IMPL(arrow_type)                       \
  arrow::Status Visit(const arrow::NumericArray<arrow_type>& array) { \
    for (int64_t i = 0; i < array.length(); i++) {                    \
      matrix_->operator()(i, 0) = encoder_->Encode(array.GetView(i)); \
    }                                                                 \
    return arrow::Status::OK();                                       \
  }
#endif  // TENSOR_TO_PMATRIX_TYPE_IMPL

class Tensor2PMatrixVisitor {
 public:
  Tensor2PMatrixVisitor() = delete;

  explicit Tensor2PMatrixVisitor(PMatrix* matrix,
                                 std::shared_ptr<PlainEncoder> encoder)
      : matrix_(matrix), encoder_(std::move(encoder)) {
    YACL_ENFORCE(matrix_, "matrix_ can not be null");
    YACL_ENFORCE(encoder_, "encoder_ can not be null");
  }

  template <typename T>
  arrow::Status Visit(const T& array) {
    return arrow::Status::NotImplemented(
        fmt::format("type {} is not implemented in Tensor2PMatrixVisitor",
                    array.type()->name()));
  }

  arrow::Status Visit(const arrow::BooleanArray& array) {
    for (int64_t i = 0; i < array.length(); i++) {
      // heu PlainEncoder not support bool
      matrix_->operator()(i, 0) =
          encoder_->Encode(static_cast<int8_t>(array.GetView(i)));
    }
    return arrow::Status::OK();
  }
  TENSOR_TO_PMATRIX_TYPE_IMPL(arrow::Int8Type);
  TENSOR_TO_PMATRIX_TYPE_IMPL(arrow::Int16Type);
  TENSOR_TO_PMATRIX_TYPE_IMPL(arrow::Int32Type);
  TENSOR_TO_PMATRIX_TYPE_IMPL(arrow::Int64Type);
  TENSOR_TO_PMATRIX_TYPE_IMPL(arrow::FloatType);
  TENSOR_TO_PMATRIX_TYPE_IMPL(arrow::DoubleType);
  // add uint32 for compatibility
  TENSOR_TO_PMATRIX_TYPE_IMPL(arrow::UInt32Type);

 private:
  PMatrix* matrix_;
  std::shared_ptr<PlainEncoder> encoder_;
};

PMatrix Tensor2PMatrix(TensorPtr t, std::shared_ptr<PlainEncoder> encoder) {
  YACL_ENFORCE(t);
  YACL_ENFORCE(t->GetNullCount() == 0, "GroupHeSum not support null yet");
  PMatrix result(t->Length(), 1);
  Tensor2PMatrixVisitor visitor(&result, std::move(encoder));

  auto array = util::ConcatenateChunkedArray(t->ToArrowChunkedArray());
  YACL_ENFORCE(array);

  THROW_IF_ARROW_NOT_OK(arrow::VisitArrayInline(*array, &visitor));

  return result;
}

std::vector<std::vector<int64_t>> BuildGroupIndices(
    const arrow::NumericArray<arrow::UInt32Type>& group_id,
    uint32_t group_num) {
  std::vector<std::vector<int64_t>> tmp(group_num, std::vector<int64_t>());
  for (int64_t i = 0; i < group_id.length(); ++i) {
    tmp[group_id.GetView(i)].push_back(i);
  }
  // drop group with empty row
  std::vector<std::vector<int64_t>> result;
  for (size_t i = 0; i < tmp.size(); ++i) {
    if (tmp[i].size() > 0) {
      result.push_back(tmp[i]);
    }
  }
  return result;
}

TensorPtr PMatrix2Tensor(const PMatrix& p, pb::PrimitiveDataType type,
                         std::shared_ptr<PlainEncoder> encoder) {
  YACL_ENFORCE(p.cols() == 1);
  TensorPtr result;
  if (type == pb::PrimitiveDataType::FLOAT64) {
    DoubleTensorBuilder builder;
    for (int row = 0; row < p.rows(); ++row) {
      builder.Append(encoder->Decode<double>(p(row, 0)));
    }
    builder.Finish(&result);
  } else if (type == pb::PrimitiveDataType::INT64) {
    Int64TensorBuilder builder;
    for (int row = 0; row < p.rows(); ++row) {
      builder.Append(encoder->Decode<int64_t>(p(row, 0)));
    }
    builder.Finish(&result);
  } else {
    YACL_THROW("not support output type={}", pb::PrimitiveDataType_Name(type));
  }

  return result;
}

}  // namespace

void GroupHeSum::Validate(ExecContext* ctx) {
  const auto& group_id = ctx->GetInput(kGroupId);
  YACL_ENFORCE(group_id.size() == 1, "group id size must be 1");
  const auto& group_num = ctx->GetInput(kGroupNum);
  YACL_ENFORCE(group_num.size() == 1, "group num size must be 1");
  const auto& inputs = ctx->GetInput(kIn);
  YACL_ENFORCE(inputs.size() == 1, "input size must be 1");
  const auto& outputs = ctx->GetOutput(kOut);
  YACL_ENFORCE(outputs.size() == inputs.size(),
               "outputs' size={} not equal to inputs' size={}", outputs.size(),
               inputs.size());

  YACL_ENFORCE(
      util::IsTensorStatusMatched(group_id[0], pb::TENSORSTATUS_PRIVATE),
      "group id status is not private");
  YACL_ENFORCE(
      util::IsTensorStatusMatched(group_num[0], pb::TENSORSTATUS_PRIVATE),
      "group num status is not private");
  YACL_ENFORCE(util::AreTensorsStatusMatched(inputs, pb::TENSORSTATUS_PRIVATE),
               "inputs' status are not all private");
  YACL_ENFORCE(util::AreTensorsStatusMatched(outputs, pb::TENSORSTATUS_PRIVATE),
               "outputs' status  are not all private");
}

void GroupHeSum::Execute(ExecContext* ctx) {
  std::vector<std::string> input_party_codes =
      ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);
  YACL_ENFORCE(input_party_codes.size() == 2,
               "attribute {} should contain 2 items", kInputPartyCodesAttr);

  const auto& party_evaluate = input_party_codes.at(0);
  const auto& party_en_de = input_party_codes.at(1);
  const auto& my_party_code = ctx->GetSession()->SelfPartyCode();

  auto lctx = ctx->GetSession()->GetLink();
  if (my_party_code == party_evaluate) {
    // prepare group id
    auto group_id_arr = GetGroupId(ctx);
    if (group_id_arr->length() == 0) {
      return;
    }
    auto group_id_ptr =
        dynamic_cast<const arrow::NumericArray<arrow::UInt32Type>*>(
            group_id_arr.get());
    YACL_ENFORCE(group_id_ptr != nullptr,
                 "failed to cast group_id_arr to NumericArray<Uint32>");
    auto group_indices = BuildGroupIndices(*group_id_ptr, GetGroupNum(ctx));
    // recv HE public key and encrypted data from party_en_de
    auto peer_rank = ctx->GetSession()->GetPartyRank(party_en_de);
    YACL_ENFORCE(peer_rank != -1, "unknown rank for party={}", party_en_de);

    yacl::Buffer pk = lctx->Recv(peer_rank, ctx->GetNodeName() + "pk");
    auto he_kit = DestinationHeKit(heu::lib::phe::DestinationHeKit(pk));

    yacl::Buffer data = lctx->Recv(peer_rank, ctx->GetNodeName() + "data");
    auto encrypted_in = DenseMatrix<Ciphertext>::LoadFrom(data);

    // calculate sum and send data back
    CMatrix encrypted_sum = CMatrix(group_indices.size(), 1);
    for (size_t i = 0; i < group_indices.size(); ++i) {
      encrypted_sum(i, 0) = he_kit.GetEvaluator()->SelectSum(
          encrypted_in, group_indices[i], std::vector<int64_t>{0});
    }
    lctx->Send(peer_rank, encrypted_sum.Serialize(),
               ctx->GetNodeName() + "sum");

  } else {
    // encrypt input
    const auto& input_pb = ctx->GetInput(kIn)[0];
    auto in_t = ctx->GetTensorTable()->GetTensor(input_pb.name());
    YACL_ENFORCE(in_t, "no input tensor={}", input_pb.name());
    if (in_t->Length() == 0) {
      // TODO(jingshi): add func to build empty Tensor with specific type, avoid
      // type mismatch
      ctx->GetTensorTable()->AddTensor(ctx->GetOutput(kOut)[0].name(), in_t);
      return;
    }
    auto encoder = std::make_shared<PlainEncoder>(GetHeSchemaType());
    auto in_p = Tensor2PMatrix(in_t, encoder);

    auto he_kit = HeKit(heu::lib::phe::HeKit(GetHeSchemaType(), 2048));
    CMatrix encrypted_in = he_kit.GetEncryptor()->Encrypt(in_p);

    // send encrypted input to party_evaluate
    auto peer_rank = ctx->GetSession()->GetPartyRank(party_evaluate);
    YACL_ENFORCE(peer_rank != -1, "unknown rank for party={}", party_evaluate);

    lctx->Send(peer_rank, he_kit.GetPublicKey()->Serialize(),
               ctx->GetNodeName() + "pk");

    lctx->Send(peer_rank, encrypted_in.Serialize(),
               ctx->GetNodeName() + "data");

    // recv encrypted sum result
    yacl::Buffer sum = lctx->Recv(peer_rank, ctx->GetNodeName() + "sum");
    auto encrypted_sum = DenseMatrix<Ciphertext>::LoadFrom(sum);
    auto plaintext_sum = he_kit.GetDecryptor()->Decrypt(encrypted_sum);

    auto t = PMatrix2Tensor(plaintext_sum, ctx->GetOutput(kOut)[0].elem_type(),
                            encoder);
    YACL_ENFORCE(t);
    ctx->GetTensorTable()->AddTensor(ctx->GetOutput(kOut)[0].name(), t);
  }
}

std::shared_ptr<arrow::Array> GroupHeSum::GetGroupId(ExecContext* ctx) {
  const auto& group = ctx->GetInput(kGroupId)[0];
  auto group_t = ctx->GetTensorTable()->GetTensor(group.name());
  YACL_ENFORCE(group_t, "no group id={}", group.name());
  return util::ConcatenateChunkedArray(group_t->ToArrowChunkedArray());
}

uint32_t GroupHeSum::GetGroupNum(ExecContext* ctx) {
  const auto& group_num = ctx->GetInput(kGroupNum)[0];
  auto group_num_t = ctx->GetTensorTable()->GetTensor(group_num.name());
  YACL_ENFORCE(group_num_t, "no group_num={}", group_num.name());
  auto num_array =
      util::ConcatenateChunkedArray(group_num_t->ToArrowChunkedArray());
  auto* num_array_ptr =
      dynamic_cast<const arrow::UInt32Array*>(num_array.get());
  YACL_ENFORCE(num_array_ptr, "cast group num to uint32_t failed");
  return num_array_ptr->Value(0);
}

};  // namespace scql::engine::op
