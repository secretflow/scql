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

#include "engine/util/psi/ub_helper.h"

#include <future>

#include "psi/utils/batch_provider.h"
#include "psi/utils/batch_provider_impl.h"
#include "yacl/crypto/rand/rand.h"

#include "engine/util/psi/batch_provider.h"

DECLARE_int64(provider_batch_size);

namespace scql::engine::util {

// OPRF ECDH PSI phases
void OprfPsiServerTransferServerItems(
    ExecContext* ctx, std::shared_ptr<yacl::link::Context> psi_link,
    const std::shared_ptr<util::BatchProvider>& batch_provider,
    const std::shared_ptr<psi::ecdh::EcdhOprfPsiServer>& ec_oprf_psi_server,
    std::shared_ptr<psi::IUbPsiCache> ub_cache) {
  auto logger = ctx->GetActiveLogger();
  SPDLOG_LOGGER_INFO(
      logger,
      "Oprf server start to transfer evaluated server items, my rank: {}, my "
      "party_code: {}",
      ctx->GetSession()->SelfRank(), ctx->GetSession()->SelfPartyCode());

  yacl::link::Barrier(psi_link, "Sync for UbPsi client and server");

  size_t self_item_count =
      ub_cache
          ? ec_oprf_psi_server->FullEvaluateAndSend(batch_provider, ub_cache)
          : ec_oprf_psi_server->FullEvaluateAndSend(batch_provider);

  SPDLOG_LOGGER_INFO(logger, "Oprf server: evaluate and send {} items",
                     self_item_count);
}

void OprfPsiServerTransferClientItems(
    ExecContext* ctx,
    const std::shared_ptr<psi::ecdh::EcdhOprfPsiServer>& ec_oprf_psi_server) {
  auto logger = ctx->GetActiveLogger();
  SPDLOG_LOGGER_INFO(
      logger,
      "Oprf server start to transfer client items, my rank: {}, my "
      "party_code: {}",
      ctx->GetSession()->SelfRank(), ctx->GetSession()->SelfPartyCode());

  ec_oprf_psi_server->RecvBlindAndSendEvaluate();
  SPDLOG_LOGGER_INFO(logger, "Oprf server finish transferring client items");
}

void OprfPsiClientTransferServerItems(
    ExecContext* ctx, std::shared_ptr<yacl::link::Context> psi_link,
    const psi::ecdh::EcdhOprfPsiOptions& psi_options,
    const std::shared_ptr<UbPsiCipherStore>& cipher_store) {
  auto logger = ctx->GetActiveLogger();
  SPDLOG_LOGGER_INFO(
      logger,
      "Oprf client start to receive evaluated server items, my rank: {}, my "
      "party_code: {}",
      ctx->GetSession()->SelfRank(), ctx->GetSession()->SelfPartyCode());
  auto ec_oprf_psi_client_offline =
      std::make_shared<psi::ecdh::EcdhOprfPsiClient>(psi_options);

  yacl::link::Barrier(psi_link, "Sync for UbPsi client and server");

  ec_oprf_psi_client_offline->RecvFinalEvaluatedItems(cipher_store);
  SPDLOG_LOGGER_INFO(
      logger,
      "Oprf client finish receiving evaluated server items, items count: {}",
      cipher_store->ItemCount());
}

void OprfPsiClientTransferClientItems(
    ExecContext* ctx,
    const std::shared_ptr<util::BatchProvider>& batch_provider,
    const psi::ecdh::EcdhOprfPsiOptions& psi_options,
    const std::shared_ptr<UbPsiCipherStore>& cipher_store) {
  auto logger = ctx->GetActiveLogger();
  SPDLOG_LOGGER_INFO(
      logger,
      "Oprf client start to transfer client items, my rank: {}, my party_code: "
      "{}",
      ctx->GetSession()->SelfRank(), ctx->GetSession()->SelfPartyCode());

  auto ec_oprf_psi_client_online =
      std::make_shared<psi::ecdh::EcdhOprfPsiClient>(psi_options);

  std::future<size_t> f_client_send_blind = std::async([&] {
    return ec_oprf_psi_client_online->SendBlindedItems(batch_provider);
  });
  ec_oprf_psi_client_online->RecvEvaluatedItems(cipher_store);
  size_t self_items_count = f_client_send_blind.get();
  SPDLOG_LOGGER_INFO(logger,
                     "Oprf client send {} blinded items in UbPsiClientOnline",
                     self_items_count);

  SPDLOG_LOGGER_INFO(
      logger, "Oprf client finish transferring client items, client_count: {}",
      cipher_store->ItemCount());
}

void OprfServerTransferShuffledClientItems(
    ExecContext* ctx,
    const std::shared_ptr<psi::ecdh::EcdhOprfPsiServer>& dh_oprf_psi_server,
    const std::string& server_cache_path,
    std::vector<uint64_t>* matched_indices, size_t* self_item_count) {
  auto logger = ctx->GetActiveLogger();
  SPDLOG_LOGGER_INFO(
      logger,
      "Oprf server start to transfer shuffled client items, my rank: {}, my "
      "party_code: {}",
      ctx->GetSession()->SelfRank(), ctx->GetSession()->SelfPartyCode());
  dh_oprf_psi_server->RecvBlindAndShuffleSendEvaluate();

  std::shared_ptr<psi::IShuffledBatchProvider> cache_provider =
      std::make_shared<psi::UbPsiCacheProvider>(server_cache_path,
                                                FLAGS_provider_batch_size);

  std::tie(*matched_indices, *self_item_count) =
      dh_oprf_psi_server->RecvIntersectionMaskedItems(cache_provider);
  SPDLOG_LOGGER_INFO(logger,
                     "Oprf server finish transfering shuffled client items");
}

void OprfCLientTransferShuffledClientItems(
    ExecContext* ctx,
    const std::shared_ptr<util::BatchProvider>& batch_provider,
    const psi::ecdh::EcdhOprfPsiOptions& psi_options,
    const std::shared_ptr<UbPsiCipherStore>& client_store,
    const std::shared_ptr<UbPsiCipherStore>& server_store) {
  auto logger = ctx->GetActiveLogger();
  SPDLOG_LOGGER_INFO(
      logger,
      "Oprf client start to transfer shuffled client items, my rank: {}, my "
      "party_code: {}",
      ctx->GetSession()->SelfRank(), ctx->GetSession()->SelfPartyCode());

  std::vector<uint8_t> private_key = yacl::crypto::RandBytes(psi::kEccKeySize);
  auto ub_psi_client_shuffle_online =
      std::make_shared<psi::ecdh::EcdhOprfPsiClient>(psi_options, private_key);

  size_t self_items_count =
      ub_psi_client_shuffle_online->SendBlindedItems(batch_provider);
  SPDLOG_LOGGER_INFO(
      logger, "Oprf client send {} blinded items in UbPsiClientShuffleOnline",
      self_items_count);

  ub_psi_client_shuffle_online->RecvEvaluatedItems(client_store);

  auto matched_items =
      FinalizeAndComputeIntersection(client_store, server_store);
  std::shared_ptr<psi::IBasicBatchProvider> intersection_masked_provider =
      std::make_shared<psi::MemoryBatchProvider>(matched_items,
                                                 FLAGS_provider_batch_size);
  ub_psi_client_shuffle_online->SendIntersectionMaskedItems(
      intersection_masked_provider);
  SPDLOG_LOGGER_INFO(logger,
                     "Oprf client finish transfering shuffled client items");
}

}  // namespace scql::engine::util
