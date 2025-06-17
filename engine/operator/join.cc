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

#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_set>
#include <vector>

#include "butil/files/scoped_temp_dir.h"
#include "msgpack.hpp"
#include "psi/algorithm/ecdh/ecdh_psi.h"
#include "psi/algorithm/ecdh/ub_psi/ecdh_oprf_psi.h"
#include "psi/algorithm/rr22/common.h"
#include "psi/algorithm/rr22/rr22_psi.h"
#include "psi/cryptor/cryptor_selector.h"
#include "yacl/crypto/rand/rand.h"

#include "engine/core/primitive_builder.h"
#include "engine/core/tensor.h"
#include "engine/core/tensor_constructor.h"
#include "engine/core/tensor_slice.h"
#include "engine/framework/exec.h"
#include "engine/util/communicate_helper.h"
#include "engine/util/filepath_helper.h"
#include "engine/util/psi/batch_provider.h"
#include "engine/util/tensor_util.h"

DECLARE_int64(provider_batch_size);

namespace scql::engine::op {

const std::string Join::kOpType("Join");

const std::string& Join::Type() const { return kOpType; }

void Join::Validate(ExecContext* ctx) {
  ValidateJoinTypeAndAlgo(ctx);

  std::vector<std::string> input_party_codes =
      ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);
  YACL_ENFORCE(input_party_codes.size() == 2,
               "Join operator attribute {} must have exactly 2 elements",
               kInputPartyCodesAttr);
}

void Join::Execute(ExecContext* ctx) {
  auto logger = ctx->GetActiveLogger();

  int64_t algorithm = ctx->GetInt64ValueFromAttribute(kAlgorithmAttr);
  switch (algorithm) {
    case static_cast<int64_t>(util::PsiAlgo::kOprfPsi): {
      SPDLOG_LOGGER_INFO(logger, "use OprfPsi for join according to attribute");
      auto server_hint = ctx->TryGetInt64ValueFromAttribute(kUbPsiServerHint);
      if (server_hint.has_value()) {
        SPDLOG_LOGGER_INFO(logger, "OprfPsi: use server hint");
        return OprfPsiJoin(ctx,
                           IsOprfServerAccordToHint(ctx, server_hint.value()));
      }
      auto psi_plan = util::CoordinatePsiPlan(ctx, true);
      return OprfPsiJoin(ctx, psi_plan.is_server, psi_plan.psi_size_info);
    }
    case static_cast<int64_t>(util::PsiAlgo::kEcdhPsi):
      SPDLOG_LOGGER_INFO(logger, "use EcdhPsi for join according to attribute");
      return Join::EcdhPsiJoin(ctx);
    // use rr22 as default
    case static_cast<int64_t>(util::PsiAlgo::kAutoPsi):
      SPDLOG_LOGGER_INFO(logger, "use Rr22Psi as default psi type");
      return Rr22PsiJoin(ctx);
    case static_cast<int64_t>(util::PsiAlgo::kRr22Psi):
      SPDLOG_LOGGER_INFO(logger, "use Rr22Psi for join according to attribute");
      return Rr22PsiJoin(ctx);
    default:
      YACL_THROW("unsupported in algorithm id: {}", algorithm);
  }
}

void Join::ValidateJoinTypeAndAlgo(ExecContext* ctx) {
  int64_t join_type = ctx->GetInt64ValueFromAttribute(kJoinTypeAttr);
  static std::unordered_set<int64_t> supported_types{
      static_cast<int64_t>(JoinType::kInnerJoin),
      static_cast<int64_t>(JoinType::kLeftJoin),
      static_cast<int64_t>(JoinType::kRightJoin)};
  YACL_ENFORCE(supported_types.count(join_type) > 0, "Invalid join type: {}",
               join_type);
  int64_t algorithm = ctx->GetInt64ValueFromAttribute(kAlgorithmAttr);
  static std::unordered_set<int64_t> supported_algorithms{
      static_cast<int64_t>(util::PsiAlgo::kAutoPsi),
      static_cast<int64_t>(util::PsiAlgo::kEcdhPsi),
      static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
      static_cast<int64_t>(util::PsiAlgo::kRr22Psi)};
  YACL_ENFORCE(supported_algorithms.count(algorithm) > 0,
               "Invalid join algorithm: {}", algorithm);
}

void Join::ValidatePsiVisibility(ExecContext* ctx) {
  const auto& left = ctx->GetInput(kInLeft);
  const auto& right = ctx->GetInput(kInRight);
  const auto& left_out = ctx->GetOutput(kOutLeftJoinIndex);
  const auto& right_out = ctx->GetOutput(kOutRightJoinIndex);
  YACL_ENFORCE(left.size() >= 1 && right.size() == left.size(),
               "Join operator inputs Left and Right should be the same and "
               "larger than 1, but got size(Left)={}, size(Right)={}",
               left.size(), right.size());
  YACL_ENFORCE(util::AreTensorsStatusMatched(left, pb::TENSORSTATUS_PRIVATE),
               "Join operator with psi-join algorithm input Left status should "
               "be private");
  YACL_ENFORCE(
      util::AreTensorsStatusMatched(right, pb::TENSORSTATUS_PRIVATE),
      "Join operator with psi-join algorithm input Right status should "
      "be private");
  YACL_ENFORCE(
      util::AreTensorsStatusMatched(left_out, pb::TENSORSTATUS_PRIVATE),
      "Join operator with psi-join algorithm output Left status should "
      "be private");
  YACL_ENFORCE(
      util::AreTensorsStatusMatched(right_out, pb::TENSORSTATUS_PRIVATE),
      "Join operator with psi-join algorithm output Right status should "
      "be private");
}

bool Join::IsOprfServerAccordToHint(ExecContext* ctx, int64_t server_hint) {
  YACL_ENFORCE(server_hint >= 0 && server_hint <= 1, "invalid server hint: {}",
               server_hint);

  const auto& my_party_code = ctx->GetSession()->SelfPartyCode();
  std::vector<std::string> input_party_codes =
      ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);
  bool is_left = my_party_code == input_party_codes.at(0);

  return (is_left && server_hint == 0) || (!is_left && server_hint == 1);
}

size_t Join::GetTargetRank(ExecContext* ctx,
                           const std::shared_ptr<yacl::link::Context>& lctx) {
  std::vector<std::string> input_party_codes =
      ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);
  const auto& my_party_code = ctx->GetSession()->SelfPartyCode();
  bool is_left = my_party_code == input_party_codes.at(0);
  if (ctx->GetOutput(kOutLeftJoinIndex).size() == 0 ||
      ctx->GetOutput(kOutRightJoinIndex).size() == 0) {
    if ((ctx->GetOutput(kOutLeftJoinIndex).size() != 0 && is_left) ||
        (ctx->GetOutput(kOutRightJoinIndex).size() != 0 && !is_left)) {
      return lctx->Rank();
    } else {
      return lctx->NextRank();
    }
  }
  return yacl::link::kAllRank;
}

bool TouchResult(const std::shared_ptr<yacl::link::Context>& lctx,
                 size_t target_rank) {
  return target_rank == yacl::link::kAllRank || lctx->Rank() == target_rank;
}

std::shared_ptr<BucketTensorConstructor> CreateBucketTensorConstructor(
    ExecContext* ctx, bool touch_result, bool is_bucket_tensor,
    const std::string& output_name, size_t bucket_num) {
  if (!touch_result) {
    return nullptr;
  }
  const auto& output_pb = ctx->GetOutput(output_name)[0];
  if (is_bucket_tensor) {
    auto streaming_options = ctx->GetSession()->GetStreamingOptions();
    std::filesystem::path out_dir = util::CreateDirWithRandSuffix(
        streaming_options.dump_file_dir, output_pb.name());
    return std::make_shared<DiskBucketTensorConstructor>(
        output_pb.name(), arrow::uint64(), output_pb.elem_type(), out_dir,
        bucket_num);
  } else {
    return std::make_shared<MemoryBucketTensorConstructor>(bucket_num);
  }
}

void Join::EcdhPsiJoin(ExecContext* ctx) {
  auto logger = ctx->GetActiveLogger();

  const auto& my_party_code = ctx->GetSession()->SelfPartyCode();
  std::vector<std::string> input_party_codes =
      ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);
  bool is_left = my_party_code == input_party_codes.at(0);
  auto join_keys = GetJoinKeys(ctx, is_left);
  if (ctx->GetSession()->GetPsiLogger()) {
    ctx->GetSession()->GetPsiLogger()->LogInput(join_keys);
  }
  std::vector<std::shared_ptr<TensorSlice>> slices(join_keys.size());
  for (size_t i = 0; i < join_keys.size(); ++i) {
    slices[i] = CreateTensorSlice(join_keys[i]);
  }
  std::string output_name = kOutLeftJoinIndex;
  if (!is_left) {
    output_name = kOutRightJoinIndex;
  }
  size_t total_self_size = 0;
  size_t total_peer_size = 0;
  auto psi_link = ctx->GetSession()->GetLink();
  if (psi_link->WorldSize() > 2) {
    psi_link = psi_link->SubWorld(ctx->GetNodeName() + "-EcdhPsiJoin",
                                  input_party_codes);
  }
  size_t target_rank = GetTargetRank(ctx, psi_link);
  bool touch_result = TouchResult(psi_link, target_rank);
  bool is_bucket_tensor = util::AreAllBucketTensor(join_keys);
  std::shared_ptr<BucketTensorConstructor> tensor_constructor =
      CreateBucketTensorConstructor(ctx, touch_result, is_bucket_tensor,
                                    output_name, slices[0]->GetSliceNum());
  for (size_t i = 0; i < slices[0]->GetSliceNum(); ++i) {
    std::vector<TensorPtr> keys(join_keys.size());
    for (size_t j = 0; j < slices.size(); ++j) {
      keys[j] = slices[j]->GetSlice(i);
    }
    auto batch_provider =
        std::make_shared<util::BatchProvider>(keys, FLAGS_provider_batch_size);
    // NOTE(shunde.csd): There are some possible ways to optimize the
    // performance of compute join indices.
    //   1. Try to adjust bins number based on the both input sizes and memory
    // amounts.
    //   2. Try to use pure memory store when the input size is small.
    // TODO: modify num of bins by data num
    auto self_store =
        std::make_shared<psi::HashBucketEcPointStore>("/tmp", util::kNumBins);
    auto peer_store =
        std::make_shared<psi::HashBucketEcPointStore>("/tmp", util::kNumBins);
    {
      psi::ecdh::EcdhPsiOptions options;
      options.link_ctx = psi_link;
      options.ecc_cryptor = psi::CreateEccCryptor(static_cast<psi::CurveType>(
          ctx->GetSession()->GetSessionOptions().psi_config.psi_curve_type));
      options.target_rank = target_rank;
      if (join_keys.size() > 0) {
        options.on_batch_finished = util::BatchFinishedCb(
            logger, ctx->GetSession()->Id(),
            (join_keys[0]->Length() + options.batch_size - 1) /
                options.batch_size);
      }
      if (ctx->GetSession()->GetPsiLogger()) {
        options.ecdh_logger =
            ctx->GetSession()->GetPsiLogger()->GetEcdhLogger();
      }
      psi::ecdh::RunEcdhPsi(options, batch_provider, self_store, peer_store);
    }
    if (touch_result) {
      YACL_ENFORCE(tensor_constructor != nullptr);
      int64_t join_type = ctx->GetInt64ValueFromAttribute(kJoinTypeAttr);
      auto join_indices = util::FinalizeAndComputeJoinIndices(
          is_left, self_store, peer_store, join_type);
      tensor_constructor->InsertBucket(join_indices, i);
    }
    total_self_size += self_store->ItemCount();
    // TODO(FIXME): @xiaoyuan fix peer size if self doesn't touch result
    total_peer_size += peer_store->ItemCount();
  }
  if (touch_result) {
    YACL_ENFORCE(tensor_constructor != nullptr);
    TensorPtr join_indices;
    tensor_constructor->Finish(&join_indices);
    auto result_size = join_indices->Length();
    SPDLOG_LOGGER_INFO(
        logger,
        "ECDH PSI Join finish, my_party_code:{}, my_rank:{}, total "
        "self_item_count:{}, total peer_item_count:{}, result_size:{}",
        ctx->GetSession()->SelfPartyCode(), ctx->GetSession()->SelfRank(),
        total_self_size, total_peer_size, result_size);
    if (ctx->GetSession()->GetPsiLogger()) {
      ctx->GetSession()->GetPsiLogger()->LogOutput(join_indices, join_keys);
    }
    SetJoinResult(ctx, is_left, std::move(join_indices));
  } else {
    SPDLOG_LOGGER_INFO(
        logger,
        "ECDH PSI Join finish, my_party_code:{}, my_rank:{}, total "
        "self_item_count:{}, total peer_item_count:{}, self doesn't touch "
        "result",
        ctx->GetSession()->SelfPartyCode(), ctx->GetSession()->SelfRank(),
        total_self_size, total_peer_size);
  }
}

void Join::Rr22PsiJoin(ExecContext* ctx) {
  auto logger = ctx->GetActiveLogger();
  const auto& my_party_code = ctx->GetSession()->SelfPartyCode();
  std::vector<std::string> input_party_codes =
      ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);

  bool is_left = my_party_code == input_party_codes.at(0);
  auto join_keys = GetJoinKeys(ctx, is_left);
  if (ctx->GetSession()->GetPsiLogger()) {
    ctx->GetSession()->GetPsiLogger()->LogInput(join_keys);
  }
  auto psi_link = ctx->GetSession()->GetLink();
  if (psi_link->WorldSize() > 2) {
    psi_link = psi_link->SubWorld(ctx->GetNodeName() + "-Rr22PsiJoin",
                                  input_party_codes);
  }
  auto target_rank = GetTargetRank(ctx, psi_link);
  bool touch_result = TouchResult(psi_link, target_rank);
  std::string output_name = kOutLeftJoinIndex;
  if (!is_left) {
    output_name = kOutRightJoinIndex;
  }
  auto self_size = static_cast<size_t>(join_keys[0]->Length());
  auto provider = util::BucketProvider(join_keys);
  auto peer_size = util::ExchangeSetSize(psi_link, self_size);
  provider.InitBucket(psi_link, self_size, peer_size);
  auto bucket_num = provider.GetBucketNum();
  std::vector<TensorPtr> result_ts(bucket_num);
  int64_t join_type = ctx->GetInt64ValueFromAttribute(kJoinTypeAttr);
  std::shared_ptr<BucketTensorConstructor> tensor_constructor =
      CreateBucketTensorConstructor(ctx, touch_result,
                                    provider.IsBucketTensor(), output_name,
                                    bucket_num);
  psi::rr22::PreProcessFunc pre_f =
      [&](size_t idx) -> std::vector<psi::HashBucketCache::BucketItem> {
    YACL_ENFORCE(idx < bucket_num);
    return provider.GetDeDupItemsInBucket(idx);
  };
  psi::rr22::PostProcessFunc post_f =
      [&](size_t bucket_idx,
          const std::vector<psi::HashBucketCache::BucketItem>& bucket_items,
          const std::vector<uint32_t>& indices,
          const std::vector<uint32_t>& peer_cnt) {
        if (touch_result) {
          YACL_ENFORCE(tensor_constructor != nullptr);
          auto indices_t = provider.CalIntersection(
              psi_link->Spawn(std::to_string(bucket_idx)), bucket_idx, is_left,
              join_type, bucket_items, indices, peer_cnt);
          tensor_constructor->InsertBucket(indices_t, bucket_idx);
        }
        provider.CleanBucket(bucket_idx);
      };
  psi::rr22::Rr22Runner runner(
      psi_link, psi::rr22::GenerateRr22PsiOptions(false), bucket_num,
      target_rank == yacl::link::kAllRank, pre_f, post_f);
  bool is_sender = is_left;
  if (target_rank != yacl::link::kAllRank) {
    // receiver get result
    is_sender = target_rank != psi_link->Rank();
  }
  runner.ParallelRun(0, is_sender);
  size_t result_size = 0;
  if (touch_result) {
    YACL_ENFORCE(tensor_constructor != nullptr);
    TensorPtr join_indices;
    tensor_constructor->Finish(&join_indices);
    result_size = join_indices->Length();
    SPDLOG_LOGGER_INFO(
        logger,
        "RR22 PSI Join finish, my_party_code:{}, my_rank:{}, total "
        "self_item_count:{}, total peer_item_count:{}, result_size:{}",
        ctx->GetSession()->SelfPartyCode(), ctx->GetSession()->SelfRank(),
        self_size, peer_size, result_size);
    SetJoinResult(ctx, is_left, std::move(join_indices));
  } else {
    SPDLOG_LOGGER_INFO(
        logger,
        "RR22 PSI Join finish, my_party_code:{}, my_rank:{}, total "
        "self_item_count:{}, total peer_item_count:{}, self doesn't touch "
        "result",
        ctx->GetSession()->SelfPartyCode(), ctx->GetSession()->SelfRank(),
        self_size, peer_size);
  }
}

void Join::OprfPsiJoin(ExecContext* ctx, bool is_server,
                       std::optional<util::PsiSizeInfo> psi_size_info) {
  auto logger = ctx->GetActiveLogger();
  util::PsiExecutionInfoTable psi_info_table;
  psi_info_table.start_time = std::chrono::system_clock::now();
  if (psi_size_info.has_value()) {
    psi_info_table.self_size = psi_size_info->self_size;
    psi_info_table.peer_size = psi_size_info->peer_size;
  } else {
    psi_info_table.self_size = 0;
    psi_info_table.peer_size = 0;
  }

  // get related party codes and ranks
  std::vector<std::string> input_party_codes =
      ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);
  const auto& my_party_code = ctx->GetSession()->SelfPartyCode();
  bool is_left = my_party_code == input_party_codes.at(0);
  const auto& peer_party_code = input_party_codes.at(is_left ? 1 : 0);
  auto my_rank = ctx->GetSession()->GetPartyRank(my_party_code);
  YACL_ENFORCE(my_rank != -1, "unknown rank for party={}", my_party_code);
  auto peer_rank = ctx->GetSession()->GetPartyRank(peer_party_code);
  YACL_ENFORCE(peer_rank != -1, "unknown rank for party={}", peer_party_code);

  // prepare input
  auto join_keys = GetJoinKeys(ctx, is_left);

  // set EcdhOprfPsiOptions
  psi::ecdh::EcdhOprfPsiOptions psi_options;
  auto psi_link = ctx->GetSession()->GetLink();
  if (psi_link->WorldSize() > 2) {
    psi_link = psi_link->SubWorld(ctx->GetNodeName() + "-OprfPsiJoin",
                                  input_party_codes);
  }
  psi_options.cache_transfer_link = psi_link;
  YACL_ENFORCE(psi_options.cache_transfer_link,
               "fail to create cache_transfer_link for OprfPsiJoin");
  psi_options.online_link = psi_options.cache_transfer_link->Spawn();
  YACL_ENFORCE(psi_options.online_link,
               "fail to create online_link for OprfPsiJoin");

  psi_options.curve_type = static_cast<psi::CurveType>(
      ctx->GetSession()->GetSessionOptions().psi_config.psi_curve_type);

  int64_t join_type = ctx->GetInt64ValueFromAttribute(kJoinTypeAttr);
  JoinRole join_role = GetJoinRole(join_type, is_left);
  std::vector<std::shared_ptr<TensorSlice>> slices(join_keys.size());
  for (size_t i = 0; i < join_keys.size(); ++i) {
    slices[i] = CreateTensorSlice(join_keys[i]);
  }
  std::string output_name = kOutLeftJoinIndex;
  if (!is_left) {
    output_name = kOutRightJoinIndex;
  }
  bool is_bucket_tensor = util::AreAllBucketTensor(join_keys);
  // ub psi always touch result
  bool touch_result = true;
  std::shared_ptr<BucketTensorConstructor> tensor_constructor =
      CreateBucketTensorConstructor(ctx, touch_result, is_bucket_tensor,
                                    output_name, slices[0]->GetSliceNum());
  for (size_t i = 0; i < slices[0]->GetSliceNum(); ++i) {
    std::vector<TensorPtr> keys(join_keys.size());
    for (size_t j = 0; j < slices.size(); ++j) {
      keys[j] = slices[j]->GetSlice(i);
    }
    // create temp dir
    butil::ScopedTempDir tmp_dir;
    YACL_ENFORCE(tmp_dir.CreateUniqueTempDir(), "fail to create temp dir");
    auto batch_provider =
        std::make_shared<util::BatchProvider>(keys, FLAGS_provider_batch_size);
    TensorPtr indices;
    if (is_server) {
      indices = OprfPsiServer(ctx, join_role, tmp_dir.path().value(),
                              psi_options, batch_provider, is_left, peer_rank,
                              &psi_info_table, psi_link);
    } else {
      indices = OprfPsiClient(ctx, join_role, tmp_dir.path().value(),
                              psi_options, batch_provider, is_left, peer_rank,
                              &psi_info_table, psi_link);
    }
    tensor_constructor->InsertBucket(indices, i);
  }
  TensorPtr join_indices;
  tensor_constructor->Finish(&join_indices);
  SetJoinResult(ctx, is_left, join_indices);
  psi_info_table.result_size = join_indices->Length();
  SPDLOG_LOGGER_INFO(
      logger,
      "OPRF PSI Join finish, my_party_code:{}, my_rank:{}, total "
      "self_item_count:{}, total peer_item_count:{}, result size:{}",
      ctx->GetSession()->SelfPartyCode(), ctx->GetSession()->SelfRank(),
      psi_info_table.self_size, psi_info_table.peer_size,
      psi_info_table.result_size);
}

std::vector<TensorPtr> Join::GetJoinKeys(ExecContext* ctx, bool is_left) {
  std::string input_name = kInLeft;
  if (!is_left) {
    input_name = kInRight;
  }

  std::vector<TensorPtr> result;
  auto* table = ctx->GetTensorTable();
  const auto& input_pbs = ctx->GetInput(input_name);
  for (const auto& input_pb : input_pbs) {
    auto t = table->GetTensor(input_pb.name());
    YACL_ENFORCE(t != nullptr, "tensor not found in tensor table",
                 input_pb.name());
    result.push_back(std::move(t));
  }
  return result;
}

void Join::SetJoinResult(ExecContext* ctx, bool is_left,
                         TensorPtr result_tensor) {
  std::string output_name = kOutLeftJoinIndex;
  if (!is_left) {
    output_name = kOutRightJoinIndex;
  }
  const auto& output_pb = ctx->GetOutput(output_name)[0];

  ctx->GetTensorTable()->AddTensor(output_pb.name(), std::move(result_tensor));
}

auto Join::GetJoinRole(int64_t join_type, bool is_left) -> JoinRole {
  if ((join_type == static_cast<int64_t>(JoinType::kLeftJoin) && is_left) ||
      (join_type == static_cast<int64_t>(JoinType::kRightJoin) && !is_left)) {
    return JoinRole::kLeftOrRightJoinFullParty;
  }
  if ((join_type == static_cast<int64_t>(JoinType::kLeftJoin) && !is_left) ||
      (join_type == static_cast<int64_t>(JoinType::kRightJoin) && is_left)) {
    return JoinRole::kLeftOrRightJoinNullParty;
  }
  if (join_type == static_cast<int64_t>(JoinType::kInnerJoin)) {
    return JoinRole::kInnerJoinParty;
  }
  return JoinRole::kInValid;
}

TensorPtr Join::OprfPsiServer(
    ExecContext* ctx, JoinRole join_role, const std::string& tmp_dir,
    const psi::ecdh::EcdhOprfPsiOptions& psi_options,
    const std::shared_ptr<util::BatchProvider>& batch_provider, bool is_left,
    int64_t peer_rank, util::PsiExecutionInfoTable* psi_info_table,
    std::shared_ptr<yacl::link::Context> psi_link) {
  std::vector<uint8_t> private_key =
      yacl::crypto::SecureRandBytes(psi::kEccKeySize);
  auto ec_oprf_psi_server =
      std::make_shared<psi::ecdh::EcdhOprfPsiServer>(psi_options, private_key);
  YACL_ENFORCE(ec_oprf_psi_server, "Fail to create EcdhOprfPsiServer");
  auto ub_cache =
      std::make_shared<util::UbPsiJoinCache>(batch_provider->TotalLength());

  auto transfer_server_items_future =
      std::async(std::launch::async, util::OprfPsiServerTransferServerItems,
                 ctx, psi_link, batch_provider, ec_oprf_psi_server, ub_cache);
  util::OprfPsiServerTransferClientItems(ctx, ec_oprf_psi_server);
  transfer_server_items_future.get();

  uint64_t client_unmatched_count = 0;
  if (join_role == JoinRole::kLeftOrRightJoinNullParty) {
    client_unmatched_count = RecvNullCount(ctx, peer_rank);
  }

  auto matched_seqs = RecvMatchedSeqs(ctx, peer_rank);
  return BuildServerResult(matched_seqs, ub_cache, client_unmatched_count,
                           join_role, batch_provider);
}

TensorPtr Join::OprfPsiClient(
    ExecContext* ctx, JoinRole join_role, const std::string& tmp_dir,
    const psi::ecdh::EcdhOprfPsiOptions& psi_options,
    const std::shared_ptr<util::BatchProvider>& batch_provider, bool is_left,
    int64_t peer_rank, util::PsiExecutionInfoTable* psi_info_table,
    std::shared_ptr<yacl::link::Context> psi_link) {
  std::string server_cipher_store_path =
      fmt::format("{}/tmp-server-cipher-store.csv", tmp_dir);
  auto server_cipher_store =
      std::make_shared<util::UbPsiCipherStore>(server_cipher_store_path, false);
  auto transfer_server_items_future =
      std::async(std::launch::async, util::OprfPsiClientTransferServerItems,
                 ctx, psi_link, psi_options, server_cipher_store);

  std::string client_cipher_store_path =
      fmt::format("{}/tmp-client-cipher-store.csv", tmp_dir);
  auto client_cipher_store =
      std::make_shared<util::UbPsiCipherStore>(client_cipher_store_path, true);
  util::OprfPsiClientTransferClientItems(ctx, batch_provider, psi_options,
                                         client_cipher_store);
  transfer_server_items_future.get();

  std::vector<uint64_t> match_server_seqs;

  TensorPtr result_tensor;
  std::vector<uint64_t> matched_seqs;

  auto lctx = ctx->GetSession()->GetLink();
  if (join_role == JoinRole::kLeftOrRightJoinFullParty) {
    uint64_t client_unmatched_count = 0;
    std::tie(result_tensor, matched_seqs) =
        util::FinalizeAndComputeOprfJoinResult(server_cipher_store,
                                               client_cipher_store, nullptr,
                                               &client_unmatched_count);
    SendNullCount(ctx, peer_rank, client_unmatched_count);
  } else if (join_role == JoinRole::kLeftOrRightJoinNullParty) {
    uint64_t server_unmatched_count = 0;
    std::tie(result_tensor, matched_seqs) =
        util::FinalizeAndComputeOprfJoinResult(
            server_cipher_store, client_cipher_store, &server_unmatched_count,
            nullptr);
  } else if (join_role == JoinRole::kInnerJoinParty) {
    std::tie(result_tensor, matched_seqs) =
        util::FinalizeAndComputeOprfJoinResult(
            server_cipher_store, client_cipher_store, nullptr, nullptr);
  } else {
    YACL_THROW("unexpected condition: join_role={}",
               static_cast<int64_t>(join_role));
  };

  SendMatchedSeqs(ctx, peer_rank, matched_seqs);
  return result_tensor;
}

uint64_t Join::RecvNullCount(ExecContext* ctx, int64_t peer_rank) {
  auto lctx = ctx->GetSession()->GetLink();
  auto tag = ctx->GetNodeName() + "-JoinNullCount";
  yacl::Buffer null_count_buf = lctx->Recv(peer_rank, tag);
  msgpack::object_handle oh = msgpack::unpack(
      static_cast<char*>(null_count_buf.data()), null_count_buf.size());
  return oh.get().as<uint64_t>();
}

void Join::SendNullCount(ExecContext* ctx, int64_t peer_rank,
                         uint64_t null_count) {
  auto lctx = ctx->GetSession()->GetLink();
  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, null_count);
  auto tag = ctx->GetNodeName() + "-JoinNullCount";
  lctx->Send(peer_rank, yacl::ByteContainerView(sbuf.data(), sbuf.size()), tag);
}

void Join::SendMatchedSeqs(ExecContext* ctx, int64_t peer_rank,
                           const std::vector<uint64_t>& matched_seqs) {
  auto logger = ctx->GetActiveLogger();
  SPDLOG_LOGGER_INFO(logger,
                     "OPRF Join: start sending matched seqs, total count: {}",
                     matched_seqs.size());

  auto lctx = ctx->GetSession()->GetLink();
  auto tag = ctx->GetNodeName() + "-UbJoinMatchedSeqs";

  util::SendMassiveMsgpack<uint64_t>(lctx, tag, peer_rank, matched_seqs);

  SPDLOG_LOGGER_INFO(logger, "OPRF Join: finish sending matched seqs");
}

std::vector<uint64_t> Join::RecvMatchedSeqs(ExecContext* ctx,
                                            int64_t peer_rank) {
  auto lctx = ctx->GetSession()->GetLink();
  auto tag = ctx->GetNodeName() + "-UbJoinMatchedSeqs";
  return util::RecvMassiveMsgpack<uint64_t>(lctx, tag, peer_rank);
}

TensorPtr Join::BuildServerResult(
    const std::vector<uint64_t>& matched_seqs,
    const std::shared_ptr<util::UbPsiJoinCache>& ub_cache,
    uint64_t client_unmatched_count, JoinRole join_role,
    const std::shared_ptr<util::BatchProvider>& batch_provider) {
  UInt64TensorBuilder result_builder;
  result_builder.Reserve(
      static_cast<int64_t>(matched_seqs.size() + client_unmatched_count));
  std::unordered_set<uint64_t> matched_indices;
  for (uint64_t matched_seq : matched_seqs) {
    uint64_t matched_indice = ub_cache->GetIndice(matched_seq);
    result_builder.UnsafeAppend(matched_indice);
    matched_indices.insert(matched_indice);
  }

  if (join_role == JoinRole::kLeftOrRightJoinNullParty) {
    for (uint64_t i = 0; i < client_unmatched_count; ++i) {
      result_builder.UnsafeAppendNull();
    }
  } else if (join_role == JoinRole::kLeftOrRightJoinFullParty) {
    for (int indice = 0; indice < batch_provider->TotalLength(); ++indice) {
      if (matched_indices.count(indice) == 0) {
        result_builder.Append(indice);
      }
    }
  }

  TensorPtr result_tensor;
  result_builder.Finish(&result_tensor);
  return result_tensor;
}

}  // namespace scql::engine::op
