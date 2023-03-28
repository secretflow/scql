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

#include "engine/operator/in.h"

#include "libspu/psi/core/ecdh_psi.h"
#include "libspu/psi/cryptor/cryptor_selector.h"

#include "engine/util/psi_helper.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

namespace {

enum InAlgo {
  SecretShareIn = 0,
  PsiIn = 1,
  LocalIn = 2,
  AlgoNums,  // Sentinel Value
};

}

const std::string In::kOpType("In");

const std::string& In::Type() const { return kOpType; }

void In::Validate(ExecContext* ctx) {
  int64_t algorithm = ctx->GetInt64ValueFromAttribute(kAlgorithmAttr);

  if (algorithm < 0 || algorithm >= InAlgo::AlgoNums) {
    YACL_THROW("Unknown in algorithm value: {}", algorithm);
  }
  const auto& left = ctx->GetInput(kInLeft);
  const auto& right = ctx->GetInput(kInRight);
  const auto& out = ctx->GetOutput(kOut);

  YACL_ENFORCE(left.size() == 1 && right.size() == 1,
               "In operator inputs Left and Right both size should be 1, but "
               "got size(Left)={}, size(Right)={}",
               left.size(), right.size());
  YACL_ENFORCE(out.size() == 1,
               "In operator output size should be 1, but got={}", out.size());
  if (algorithm == InAlgo::PsiIn) {
    // reveal_to must have one element
    const auto& input_party_codes =
        ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);
    YACL_ENFORCE(input_party_codes.size() == 2,
                 "invalid attribute {} value size, expect 2 but got={}",
                 kInputPartyCodesAttr, input_party_codes.size());

    const auto& reveal_to = ctx->GetStringValuesFromAttribute(kRevealToAttr);
    YACL_ENFORCE(reveal_to.size() == 1,
                 "In operator with psi-in algorithm should only reveal to 1 "
                 "party, but got={}",
                 reveal_to.size());
    YACL_ENFORCE(util::AreTensorsStatusMatched(left, pb::TENSORSTATUS_PRIVATE),
                 "In operator with psi-in algorithm input Left status should "
                 "be private");
    YACL_ENFORCE(util::AreTensorsStatusMatched(right, pb::TENSORSTATUS_PRIVATE),
                 "In operator with psi-in algorithm input Right status should "
                 "be private");
    YACL_ENFORCE(util::AreTensorsStatusMatched(out, pb::TENSORSTATUS_PRIVATE),
                 "In operator with psi-in algorithm output status should "
                 "be private");
  } else {
    // TODO(shunde.csd): implement other in algorithm
    YACL_THROW("tobe implemented");
  }
}

void In::Execute(ExecContext* ctx) {
  int64_t algorithm = ctx->GetInt64ValueFromAttribute(kAlgorithmAttr);
  if (algorithm == InAlgo::SecretShareIn) {
    return SecretShareIn(ctx);
  } else if (algorithm == InAlgo::PsiIn) {
    return PsiIn(ctx);
  } else if (algorithm == InAlgo::LocalIn) {
    return LocalIn(ctx);
  }

  YACL_THROW("unsupported in algorithm: {}", algorithm);
}

void In::SecretShareIn(ExecContext* ctx) { YACL_THROW("unimplemented"); }

void In::PsiIn(ExecContext* ctx) {
  const auto& my_party_code = ctx->GetSession()->SelfPartyCode();

  std::vector<std::string> input_party_codes =
      ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);
  std::string reveal_to = ctx->GetStringValueFromAttribute(kRevealToAttr);

  bool is_left = my_party_code == input_party_codes.at(0);
  auto target_rank = ctx->GetSession()->GetPartyRank(reveal_to);
  YACL_ENFORCE(target_rank != -1, "unknown rank for party {}", reveal_to);

  auto param_name = ctx->GetInput(kInLeft)[0].name();
  if (!is_left) {
    param_name = ctx->GetInput(kInRight)[0].name();
  }
  auto in_tensor = ctx->GetTensorTable()->GetTensor(param_name);
  YACL_ENFORCE(in_tensor != nullptr, "{} not found in tensor table",
               param_name);

  auto batch_provider =
      std::make_shared<util::BatchProvider>(std::vector<TensorPtr>{in_tensor});
  auto in_cipher_store = std::make_shared<util::InCipherStore>("/tmp", 64);
  {
    spu::psi::EcdhPsiOptions options;
    options.link_ctx = ctx->GetSession()->GetLink();
    if (options.link_ctx->WorldSize() > 2) {
      options.link_ctx =
          options.link_ctx->SubWorld(ctx->GetNodeName(), input_party_codes);
      // update target rank since link_ctx changed.
      if (reveal_to == input_party_codes[0]) {
        target_rank = 0;
      } else {
        target_rank = 1;
      }
    }
    options.ecc_cryptor = spu::psi::CreateEccCryptor(spu::psi::CURVE_25519);
    options.target_rank = target_rank;
    options.on_batch_finished = util::BatchFinishedCb(
        ctx->GetSession()->Id(),
        (in_tensor->Length() + options.batch_size - 1) / options.batch_size);

    spu::psi::RunEcdhPsi(options, batch_provider, in_cipher_store);
  }
  // reveal to me
  if (reveal_to == my_party_code) {
    auto result = in_cipher_store->FinalizeAndComputeInResult(is_left);
    SPDLOG_INFO(
        "my_party_code:{}, my_rank:{}, total self_item_count:{}, "
        "total peer_item_count:{}, result size:{}",
        ctx->GetSession()->SelfPartyCode(), ctx->GetSession()->SelfRank(),
        in_cipher_store->GetSelfItemCount(),
        in_cipher_store->GetPeerItemCount(), result->Length());

    const auto& output_pb = ctx->GetOutput(kOut)[0];
    ctx->GetSession()->GetTensorTable()->AddTensor(output_pb.name(),
                                                   std::move(result));
  }
}

void In::LocalIn(ExecContext* ctx) { YACL_THROW("unimplemented"); }

}  // namespace scql::engine::op