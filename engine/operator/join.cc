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

#include "engine/operator/join.h"

#include "libspu/psi/core/ecdh_psi.h"
#include "libspu/psi/cryptor/cryptor_selector.h"

#include "engine/util/psi_helper.h"

namespace scql::engine::op {

const std::string Join::kOpType("Join");

const std::string& Join::Type() const { return kOpType; }

void Join::Validate(ExecContext* ctx) {
  int64_t join_type = ctx->GetInt64ValueFromAttribute(kJoinTypeAttr);

  if (join_type != kInnerJoin) {
    YACL_THROW("unsupported join_type {}", join_type);
  }

  std::vector<std::string> input_party_codes =
      ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);
  YACL_ENFORCE(input_party_codes.size() == 2,
               "Join operator attribute {} must have exactly 2 elements",
               kInputPartyCodesAttr);

  // TODO(shunde.csd): check visibility of inputs
}

void Join::Execute(ExecContext* ctx) {
  const auto& my_party_code = ctx->GetSession()->SelfPartyCode();

  std::vector<std::string> input_party_codes =
      ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);
  bool is_left = my_party_code == input_party_codes.at(0);

  auto join_keys = GetJoinKeys(ctx, is_left);

  auto batch_provider = std::make_shared<util::BatchProvider>(join_keys);
  // NOTE(shunde.csd): There are some possible ways to optimize the performance
  // of compute join indices.
  //   1. Try to adjust bins number based on the both input sizes and memory
  // amounts.
  //   2. Try to use pure memory store when the input size is small.
  auto join_cipher_store = std::make_shared<util::JoinCipherStore>("/tmp", 64);
  {
    spu::psi::EcdhPsiOptions options;
    options.link_ctx = ctx->GetSession()->GetLink();
    if (options.link_ctx->WorldSize() > 2) {
      options.link_ctx =
          options.link_ctx->SubWorld(ctx->GetNodeName(), input_party_codes);
    }
    options.ecc_cryptor = spu::psi::CreateEccCryptor(spu::psi::CURVE_25519);
    options.target_rank = yacl::link::kAllRank;
    if (join_keys.size() > 0) {
      options.on_batch_finished = util::BatchFinishedCb(
          ctx->GetSession()->Id(),
          (join_keys[0]->Length() + options.batch_size - 1) /
              options.batch_size);
    }

    spu::psi::RunEcdhPsi(options, batch_provider, join_cipher_store);
  }
  auto join_indices = join_cipher_store->FinalizeAndComputeJoinIndices(is_left);
  SPDLOG_INFO(
      "my_party_code:{}, my_rank:{}, total self_item_count:{}, "
      "total peer_item_count:{}, result_size:{}",
      ctx->GetSession()->SelfPartyCode(), ctx->GetSession()->SelfRank(),
      join_cipher_store->GetSelfItemCount(),
      join_cipher_store->GetPeerItemCount(), join_indices->Length());

  SetJoinIndices(ctx, is_left, std::move(join_indices));
}

std::vector<TensorPtr> Join::GetJoinKeys(ExecContext* ctx, bool is_left) {
  std::string input_name = kInLeft;
  if (!is_left) {
    input_name = kInRight;
  }

  std::vector<TensorPtr> result;
  auto table = ctx->GetTensorTable();
  const auto& input_pbs = ctx->GetInput(input_name);
  for (const auto& input_pb : input_pbs) {
    auto t = table->GetTensor(input_pb.name());
    YACL_ENFORCE(t != nullptr, "tensor not found in tensor table",
                 input_pb.name());
    result.push_back(std::move(t));
  }
  return result;
}

void Join::SetJoinIndices(ExecContext* ctx, bool is_left, TensorPtr indices) {
  std::string output_name = kOutLeftJoinIndex;
  if (!is_left) {
    output_name = kOutRightJoinIndex;
  }
  const auto& output_pb = ctx->GetOutput(output_name)[0];

  ctx->GetTensorTable()->AddTensor(output_pb.name(), indices);
}

}  // namespace scql::engine::op