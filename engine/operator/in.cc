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

#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "butil/files/scoped_temp_dir.h"
#include "psi/cryptor/cryptor_selector.h"
#include "psi/ecdh/ecdh_oprf_psi.h"
#include "psi/ecdh/ecdh_psi.h"
#include "psi/utils/ec_point_store.h"
#include "yacl/crypto/rand/rand.h"

#include "engine/audit/audit_log.h"
#include "engine/core/arrow_helper.h"
#include "engine/core/primitive_builder.h"
#include "engine/core/tensor.h"
#include "engine/framework/exec.h"
#include "engine/util/psi_detail_logger.h"
#include "engine/util/psi_helper.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string In::kOpType("In");

const std::string& In::Type() const { return kOpType; }

void In::Validate(ExecContext* ctx) {
  int64_t in_type = ctx->GetInt64ValueFromAttribute(kInType);
  if (in_type < 0 || in_type >= static_cast<int64_t>(InType::kInTypeNums)) {
    YACL_THROW("Unknown in type: {}", in_type);
  }

  if (in_type == static_cast<int64_t>(InType::kPsiIn)) {
    int64_t algorithm = ctx->GetInt64ValueFromAttribute(kAlgorithmAttr);
    if (algorithm < 0 ||
        algorithm >= static_cast<int64_t>(util::PsiAlgo::kAlgoNums)) {
      YACL_THROW("Unknown psi algorithm value: {}", algorithm);
    }
    ValidateInputAndOutputForPsi(ctx);
    ValidatePartyCodesForPsi(ctx);
  } else {
    // TODO(shunde.csd): implement other in algorithm
    YACL_THROW("to be implemented");
  }
}

void In::Execute(ExecContext* ctx) {
  auto logger = ctx->GetActiveLogger();
  int64_t in_type = ctx->GetInt64ValueFromAttribute(kInType);
  switch (in_type) {
    case static_cast<int64_t>(InType::kSecretShareIn):
      SPDLOG_LOGGER_INFO(logger, "Execute In, In type = {}", "SecretShareIn");
      return SecretShareIn(ctx);
    case static_cast<int64_t>(InType::kPsiIn):
      // engines will coordinate proper PSI algorithm(EcdhPsi or OprfPsi)
      // according to the tensor size
      SPDLOG_LOGGER_INFO(logger, "Execute In, In type = {}", "PsiIn");
      {
        auto algorithm = ctx->GetInt64ValueFromAttribute(kAlgorithmAttr);
        switch (algorithm) {
          case static_cast<int64_t>(util::PsiAlgo::kOprfPsi):
            SPDLOG_LOGGER_INFO(logger, "Execute In, algo = {}", "OprfPsi");
            // Besides letting the engines decide, Driver can directly choose
            // prefered PSI algorithm(EcdhPsi or OprfPsi) UbPsiServerHint is
            // indispensable when Driver explicitly choose OprfPsiIn
            return OprfPsiIn(ctx, IsOprfServerAccordToHint(ctx));
          case static_cast<int64_t>(util::PsiAlgo::kEcdhPsi):
            SPDLOG_LOGGER_INFO(logger, "Execute In, algo = {}", "EcdhPsi");
            return EcdhPsiIn(ctx);
          case static_cast<int64_t>(util::PsiAlgo::kAutoPsi):
            SPDLOG_LOGGER_INFO(logger, "Execute In, algo = {}", "AutoPsi");
            return PsiIn(ctx);
          default:
            YACL_THROW("unsupported in algorithm id: {}", algorithm);
        }
      }
    case static_cast<int64_t>(InType::kLocalIn):
      SPDLOG_LOGGER_INFO(logger, "Execute In, In type = {}", "LocalIn");
      return LocalIn(ctx);
    default:
      YACL_THROW("unsupported In type id: {}", in_type);
  }
}

void In::ValidateInputAndOutputForPsi(ExecContext* ctx) {
  const auto& left = ctx->GetInput(kInLeft);
  const auto& right = ctx->GetInput(kInRight);
  const auto& out = ctx->GetOutput(kOut);

  // operator In only supports the comparison of one column
  YACL_ENFORCE(left.size() == 1 && right.size() == 1,
               "In operator inputs Left and Right both size should be 1, but "
               "got size(Left)={}, size(Right)={}",
               left.size(), right.size());
  YACL_ENFORCE(out.size() == 1,
               "In operator output size should be 1, but got={}", out.size());

  // check tensor status
  YACL_ENFORCE(util::AreTensorsStatusMatched(left, pb::TENSORSTATUS_PRIVATE),
               "In operator with psi-in algorithm input Left status should "
               "be private");
  YACL_ENFORCE(util::AreTensorsStatusMatched(right, pb::TENSORSTATUS_PRIVATE),
               "In operator with psi-in algorithm input Right status should "
               "be private");
  YACL_ENFORCE(util::AreTensorsStatusMatched(out, pb::TENSORSTATUS_PRIVATE),
               "In operator with psi-in algorithm output status should "
               "be private");
}

void In::ValidatePartyCodesForPsi(ExecContext* ctx) {
  // only support 2 party PSI in this implementation
  const auto& input_party_codes =
      ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);
  YACL_ENFORCE(input_party_codes.size() == 2,
               "invalid attribute {} value size, expect 2 but got={}",
               kInputPartyCodesAttr, input_party_codes.size());

  const auto& reveal_to = ctx->GetStringValuesFromAttribute(kRevealToAttr);
  // reveal_to must have one element
  YACL_ENFORCE(reveal_to.size() == 1,
               "In operator with psi-in algorithm should only reveal to 1 "
               "party, but got={}",
               reveal_to.size());
  // InAlgo::PsiIn supports revealing only to the right party, but this action
  // itself is meaningless
  YACL_ENFORCE(reveal_to[0] == input_party_codes[0],
               "In result should only reveal to left party");
}

bool In::IsOprfServerAccordToHint(ExecContext* ctx) {
  auto server_hint = ctx->GetInt64ValueFromAttribute(kUbPsiServerHint);
  YACL_ENFORCE(server_hint >= 0 && server_hint <= 1, "invalid server hint: {}",
               server_hint);

  const auto& my_party_code = ctx->GetSession()->SelfPartyCode();
  std::vector<std::string> input_party_codes =
      ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);
  bool is_left = my_party_code == input_party_codes.at(0);

  return (is_left && server_hint == 0) || (!is_left && server_hint == 1);
}

void In::PsiIn(ExecContext* ctx) {
  auto logger = ctx->GetActiveLogger();
  auto psi_plan = util::CoordinatePsiPlan(ctx);
  if (psi_plan.unbalanced) {
    SPDLOG_LOGGER_INFO(logger, "OprfPsi is chosen");
    return OprfPsiIn(ctx, psi_plan.is_server, psi_plan.psi_size_info);
  } else {
    SPDLOG_LOGGER_INFO(logger, "EcdhPsi is chosen");
    return EcdhPsiIn(ctx);
  }
}

void In::OprfPsiIn(ExecContext* ctx, bool is_server,
                   std::optional<util::PsiSizeInfo> psi_size_info) {
  auto logger = ctx->GetActiveLogger();
  util::PsiExecutionInfoTable psi_info_table;
  psi_info_table.start_time = std::chrono::system_clock::now();
  // a temporary solution, related SPU-codes need to be modified someday
  if (psi_size_info.has_value()) {
    psi_info_table.self_size = psi_size_info->self_size;
    psi_info_table.peer_size = psi_size_info->peer_size;
  } else {
    psi_info_table.self_size = 0;
    psi_info_table.peer_size = 0;
  }

  const auto& my_party_code = ctx->GetSession()->SelfPartyCode();
  std::vector<std::string> input_party_codes =
      ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);
  bool is_left = my_party_code == input_party_codes.at(0);

  // prepare input
  const auto* input_name = is_left ? kInLeft : kInRight;
  auto param_name = ctx->GetInput(input_name)[0].name();
  auto in_tensor = ctx->GetTensorTable()->GetTensor(param_name);
  YACL_ENFORCE(in_tensor != nullptr, "{} not found in tensor table",
               param_name);
  auto batch_provider = std::make_shared<util::BatchProvider>(
      std::vector<TensorPtr>{in_tensor}, FLAGS_provider_batch_size);

  // check reveal condition
  std::string reveal_to_party_code =
      ctx->GetStringValueFromAttribute(kRevealToAttr);
  YACL_ENFORCE(reveal_to_party_code == input_party_codes[0],
               "In result should only reveal to left party");
  auto target_rank = ctx->GetSession()->GetPartyRank(reveal_to_party_code);
  YACL_ENFORCE(target_rank != -1, "unknown rank for party {}",
               reveal_to_party_code);
  bool reveal_to_me = reveal_to_party_code == my_party_code;
  bool reveal_to_server =
      (is_server && reveal_to_me) || (!is_server && !reveal_to_me);

  // set EcdhOprfPsiOptions
  psi::ecdh::EcdhOprfPsiOptions psi_options;
  auto psi_link = ctx->GetSession()->GetLink();
  if (psi_link->WorldSize() > 2) {
    psi_link = psi_link->SubWorld(ctx->GetNodeName() + "-OprfPsiIn",
                                  input_party_codes);
  }
  psi_options.link0 = psi_link;
  YACL_ENFORCE(psi_options.link0, "fail to getlink0 for OprfPsiIn");
  psi_options.link1 = psi_options.link0->Spawn();
  YACL_ENFORCE(psi_options.link1, "fail to getlink1 for OprfPsiIn");

  psi_options.curve_type = static_cast<psi::CurveType>(
      ctx->GetSession()->GetSessionOptions().psi_config.psi_curve_type);

  // create temp dir
  butil::ScopedTempDir tmp_dir;
  YACL_ENFORCE(tmp_dir.CreateUniqueTempDir(), "fail to create temp dir");

  if (is_server) {
    OprfPsiServer(ctx, reveal_to_server, tmp_dir.path().value(), psi_options,
                  batch_provider, &psi_info_table, psi_link);
  } else {
    OprfPsiClient(ctx, reveal_to_server, tmp_dir.path().value(), psi_options,
                  batch_provider, &psi_info_table, psi_link);
  }

  SPDLOG_LOGGER_INFO(
      logger,
      "OPRF PSI In finish, my_party_code:{}, my_rank:{}, total "
      "self_item_count:{}, total peer_item_count:{}, result size:{}",
      ctx->GetSession()->SelfPartyCode(), ctx->GetSession()->SelfRank(),
      psi_info_table.self_size, psi_info_table.peer_size,
      psi_info_table.result_size);
  audit::RecordInNodeDetail(
      *ctx, static_cast<int64_t>(psi_info_table.self_size),
      static_cast<int64_t>(psi_info_table.peer_size),
      psi_info_table.result_size, psi_info_table.start_time);
}

int64_t In::OprfServerHandleResult(ExecContext* ctx,
                                   const std::vector<uint64_t>& matched_indices,
                                   size_t self_item_count) {
  auto logger = ctx->GetActiveLogger();
  SPDLOG_LOGGER_INFO(
      logger,
      "Server handle result, matched_indices size={}, self_item_count={}",
      matched_indices.size(), self_item_count);
  std::unordered_set<uint64_t> matched_indices_set(matched_indices.begin(),
                                                   matched_indices.end());
  BooleanTensorBuilder result_builder;
  result_builder.Reserve(static_cast<int64_t>(self_item_count));
  for (uint64_t indice = 0; indice < self_item_count; ++indice) {
    if (matched_indices_set.count(indice) > 0) {
      result_builder.UnsafeAppend(true);
    } else {
      result_builder.UnsafeAppend(false);
    }
  }

  TensorPtr result_tensor;
  result_builder.Finish(&result_tensor);
  int64_t result_size = result_tensor->Length();

  const auto& output_pb = ctx->GetOutput(In::kOut)[0];
  ctx->GetSession()->GetTensorTable()->AddTensor(output_pb.name(),
                                                 std::move(result_tensor));
  return result_size;
}

void In::OprfPsiServer(
    ExecContext* ctx, bool reveal_to_server, const std::string& tmp_dir,
    const psi::ecdh::EcdhOprfPsiOptions& psi_options,
    const std::shared_ptr<util::BatchProvider>& batch_provider,
    util::PsiExecutionInfoTable* psi_info_table,
    std::shared_ptr<yacl::link::Context> psi_link) {
  std::vector<uint8_t> private_key =
      yacl::crypto::SecureRandBytes(psi::kEccKeySize);
  auto ec_oprf_psi_server =
      std::make_shared<psi::ecdh::EcdhOprfPsiServer>(psi_options, private_key);
  YACL_ENFORCE(ec_oprf_psi_server, "Fail to create EcdhOprfPsiServer");
  if (reveal_to_server) {
    // Create UbPsiCache
    std::string server_cache_path = fmt::format("{}/tmp-server-cache", tmp_dir);
    std::shared_ptr<psi::IUbPsiCache> ub_cache;
    std::vector<std::string> dummy_fields{};
    ub_cache = std::make_shared<psi::UbPsiCache>(
        server_cache_path, ec_oprf_psi_server->GetCompareLength(),
        dummy_fields);

    util::OprfPsiServerTransferServerItems(ctx, psi_link, batch_provider,
                                           ec_oprf_psi_server, ub_cache);

    std::vector<uint64_t> matched_indices;
    size_t self_item_count{};
    util::OprfServerTransferShuffledClientItems(
        ctx, ec_oprf_psi_server, server_cache_path, &matched_indices,
        &self_item_count);
    psi_info_table->result_size =
        OprfServerHandleResult(ctx, matched_indices, self_item_count);
  } else {
    auto transfer_server_items_future =
        std::async(std::launch::async, util::OprfPsiServerTransferServerItems,
                   ctx, psi_link, batch_provider, ec_oprf_psi_server, nullptr);
    util::OprfPsiServerTransferClientItems(ctx, ec_oprf_psi_server);
    transfer_server_items_future.wait();
    psi_info_table->result_size = 0;
  }
}

void In::OprfPsiClient(
    ExecContext* ctx, bool reveal_to_server, const std::string& tmp_dir,
    const psi::ecdh::EcdhOprfPsiOptions& psi_options,
    const std::shared_ptr<util::BatchProvider>& batch_provider,
    util::PsiExecutionInfoTable* psi_info_table,
    std::shared_ptr<yacl::link::Context> psi_link) {
  std::string server_cipher_store_path =
      fmt::format("{}/tmp-server-cipher-store.csv", tmp_dir);
  auto server_store =
      std::make_shared<util::UbPsiCipherStore>(server_cipher_store_path, false);

  std::string client_cipher_store_path =
      fmt::format("{}/tmp-client-cipher-store.csv", tmp_dir);
  auto client_store =
      std::make_shared<util::UbPsiCipherStore>(client_cipher_store_path, true);

  if (reveal_to_server) {
    util::OprfPsiClientTransferServerItems(ctx, psi_link, psi_options,
                                           server_store);
    util::OprfCLientTransferShuffledClientItems(
        ctx, batch_provider, psi_options, client_store, server_store);
    psi_info_table->result_size = 0;
  } else {
    auto transfer_server_items_future =
        std::async(std::launch::async, util::OprfPsiClientTransferServerItems,
                   ctx, psi_link, psi_options, server_store);
    OprfPsiClientTransferClientItems(ctx, batch_provider, psi_options,
                                     client_store);
    transfer_server_items_future.wait();
    psi_info_table->result_size =
        OprfClientHandleResult(ctx, client_store, server_store);
  }
}

int64_t In::OprfClientHandleResult(
    ExecContext* ctx,
    const std::shared_ptr<util::UbPsiCipherStore>& client_store,
    const std::shared_ptr<util::UbPsiCipherStore>& server_store) {
  auto result_tensor =
      util::FinalizeAndComputeOprfInResult(client_store, server_store);
  int64_t result_size = result_tensor->Length();

  const auto& output_pb = ctx->GetOutput(In::kOut)[0];
  ctx->GetSession()->GetTensorTable()->AddTensor(output_pb.name(),
                                                 std::move(result_tensor));
  return result_size;
}

void In::EcdhPsiIn(ExecContext* ctx) {
  const auto start_time = std::chrono::system_clock::now();
  auto logger = ctx->GetActiveLogger();

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
  if (ctx->GetSession()->GetPsiLogger()) {
    ctx->GetSession()->GetPsiLogger()->LogInput({in_tensor});
  }
  auto batch_provider = std::make_shared<util::BatchProvider>(
      std::vector<TensorPtr>{in_tensor}, FLAGS_provider_batch_size);
  auto self_store =
      std::make_shared<psi::HashBucketEcPointStore>("/tmp", util::kNumBins);
  auto peer_store =
      std::make_shared<psi::HashBucketEcPointStore>("/tmp", util::kNumBins);
  {
    psi::ecdh::EcdhPsiOptions options;
    options.link_ctx = ctx->GetSession()->GetLink();
    if (options.link_ctx->WorldSize() > 2) {
      options.link_ctx = options.link_ctx->SubWorld(
          ctx->GetNodeName() + "-EcdhPsiIn", input_party_codes);
      // update target rank since link_ctx changed.
      if (reveal_to == input_party_codes[0]) {
        target_rank = 0;
      } else {
        target_rank = 1;
      }
    }

    options.ecc_cryptor = psi::CreateEccCryptor(static_cast<psi::CurveType>(
        ctx->GetSession()->GetSessionOptions().psi_config.psi_curve_type));
    options.target_rank = target_rank;
    options.on_batch_finished = util::BatchFinishedCb(
        logger, ctx->GetSession()->Id(),
        (in_tensor->Length() + options.batch_size - 1) / options.batch_size);
    if (ctx->GetSession()->GetPsiLogger()) {
      options.ecdh_logger = ctx->GetSession()->GetPsiLogger()->GetEcdhLogger();
    }

    psi::ecdh::RunEcdhPsi(options, batch_provider, self_store, peer_store);
  }
  // reveal to me

  size_t self_size = 0;
  size_t peer_size = 0;
  int64_t result_size = 0;
  if (reveal_to == my_party_code) {
    auto result =
        util::FinalizeAndComputeInResult(is_left, self_store, peer_store);
    self_size = self_store->ItemCount();
    peer_size = peer_store->ItemCount();
    result_size = result->Length();
    SPDLOG_LOGGER_INFO(
        logger,
        "ECDH PSI In finish, my_party_code:{}, my_rank:{}, total "
        "self_item_count:{}, total peer_item_count:{}, result size:{}",
        ctx->GetSession()->SelfPartyCode(), ctx->GetSession()->SelfRank(),
        self_size, peer_size, result_size);
    const auto& output_pb = ctx->GetOutput(kOut)[0];
    if (ctx->GetSession()->GetPsiLogger()) {
      ctx->GetSession()->GetPsiLogger()->LogOutput(result);
    }
    ctx->GetSession()->GetTensorTable()->AddTensor(output_pb.name(),
                                                   std::move(result));
  }
  audit::RecordInNodeDetail(*ctx, static_cast<int64_t>(self_size),
                            static_cast<int64_t>(peer_size), result_size,
                            start_time);
}

void In::LocalIn(ExecContext* ctx) { YACL_THROW("unimplemented"); }
void In::SecretShareIn(ExecContext* ctx) { YACL_THROW("unimplemented"); }

}  // namespace scql::engine::op
