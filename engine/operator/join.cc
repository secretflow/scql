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
#include <memory>
#include <optional>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "butil/files/scoped_temp_dir.h"
#include "gflags/gflags.h"
#include "msgpack.hpp"
#include "psi/psi/core/ecdh_oprf_psi.h"
#include "psi/psi/core/ecdh_psi.h"
#include "psi/psi/cryptor/cryptor_selector.h"
#include "yacl/crypto/utils/rand.h"

#include "engine/audit/audit_log.h"
#include "engine/core/primitive_builder.h"
#include "engine/core/tensor.h"
#include "engine/framework/exec.h"
#include "engine/util/psi_helper.h"
#include "engine/util/tensor_util.h"

DECLARE_int32(psi_curve_type);

namespace scql::engine::op {

const std::string Join::kOpType("Join");

const std::string& Join::Type() const { return kOpType; }

void Join::Validate(ExecContext* ctx) {
  ValidateJoinType(ctx);

  std::vector<std::string> input_party_codes =
      ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);
  YACL_ENFORCE(input_party_codes.size() == 2,
               "Join operator attribute {} must have exactly 2 elements",
               kInputPartyCodesAttr);

  int64_t algorithm =
      ctx->TryGetInt64ValueFromAttribute(kAlgorithmAttr)
          .value_or(static_cast<int64_t>(JoinAlgo::kEcdhPsiJoin));
  if (algorithm == static_cast<int64_t>(JoinAlgo::kEcdhPsiJoin) ||
      algorithm == static_cast<int64_t>(JoinAlgo::kOprfPsiJoin)) {
    ValidatePsiVisibility(ctx);
  } else {
    YACL_THROW("to be implemented");
  }
}

void Join::Execute(ExecContext* ctx) {
  auto algorithm = ctx->TryGetInt64ValueFromAttribute(kAlgorithmAttr);

  // use server hint
  if (algorithm.has_value()) {
    SPDLOG_INFO("use server hint");
    if (algorithm == static_cast<int64_t>(JoinAlgo::kEcdhPsiJoin)) {
      return Join::EcdhPsiJoin(ctx);
    } else if (algorithm == static_cast<int64_t>(JoinAlgo::kOprfPsiJoin)) {
      return OprfPsiJoin(ctx, IsOprfServerAccordToHint(ctx));
    } else {
      YACL_THROW("unsupported join algorithm id: {}", algorithm.value());
    }
  }

  // coordinate between engines
  SPDLOG_INFO("coordinate between engines");
  util::PsiPlan psi_plan = util::CoordinatePsiPlan(ctx);
  std::string server_info;
  if (psi_plan.unbalanced) {
    if (psi_plan.is_server) {
      server_info = ", is server";
    } else {
      server_info = ", is client";
    }
  }
  SPDLOG_INFO("coordinate finished, is unbalanced: {} {}", psi_plan.unbalanced,
              server_info);
  if (psi_plan.unbalanced) {
    return Join::OprfPsiJoin(ctx, psi_plan.is_server, psi_plan.psi_size_info);
  } else {
    return Join::EcdhPsiJoin(ctx);
  }
}

void Join::ValidateJoinType(ExecContext* ctx) {
  int64_t join_type = ctx->GetInt64ValueFromAttribute(kJoinTypeAttr);
  static std::unordered_set<int64_t> supported_types{
      static_cast<int64_t>(JoinType::kInnerJoin),
      static_cast<int64_t>(JoinType::kLeftJoin),
      static_cast<int64_t>(JoinType::kRightJoin)};
  YACL_ENFORCE(supported_types.count(join_type) > 0, "Invalid join type: {}",
               join_type);
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

bool Join::IsOprfServerAccordToHint(ExecContext* ctx) {
  auto server_hint = ctx->GetInt64ValueFromAttribute(kUbPsiServerHint);
  YACL_ENFORCE(server_hint >= 0 && server_hint <= 1, "invalid server hint: {}",
               server_hint);

  const auto& my_party_code = ctx->GetSession()->SelfPartyCode();
  std::vector<std::string> input_party_codes =
      ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);
  bool is_left = my_party_code == input_party_codes.at(0);

  return (is_left && server_hint == 0) || (!is_left && server_hint == 1);
}

void Join::EcdhPsiJoin(ExecContext* ctx) {
  const auto start_time = std::chrono::system_clock::now();
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
  auto self_store = std::make_shared<psi::psi::HashBucketEcPointStore>(
      "/tmp", util::kNumBins);
  auto peer_store = std::make_shared<psi::psi::HashBucketEcPointStore>(
      "/tmp", util::kNumBins);
  {
    psi::psi::EcdhPsiOptions options;
    options.link_ctx = ctx->GetSession()->GetLink();
    if (options.link_ctx->WorldSize() > 2) {
      options.link_ctx = options.link_ctx->SubWorld(
          ctx->GetNodeName() + "-EcdhPsiJoin", input_party_codes);
    }
    options.ecc_cryptor = psi::psi::CreateEccCryptor(
        static_cast<psi::psi::CurveType>(FLAGS_psi_curve_type));
    options.target_rank = yacl::link::kAllRank;
    if (join_keys.size() > 0) {
      options.on_batch_finished = util::BatchFinishedCb(
          ctx->GetSession()->Id(),
          (join_keys[0]->Length() + options.batch_size - 1) /
              options.batch_size);
    }

    psi::psi::RunEcdhPsi(options, batch_provider, self_store, peer_store);
  }

  int64_t join_type = ctx->GetInt64ValueFromAttribute(kJoinTypeAttr);
  auto join_indices = util::FinalizeAndComputeJoinIndices(
      is_left, self_store, peer_store, join_type);
  auto self_size = self_store->ItemCount();
  auto peer_size = peer_store->ItemCount();
  auto result_size = join_indices->Length();
  SPDLOG_INFO(
      "ECDH PSI Join finish, my_party_code:{}, my_rank:{}, total "
      "self_item_count:{}, total peer_item_count:{}, result_size:{}",
      ctx->GetSession()->SelfPartyCode(), ctx->GetSession()->SelfRank(),
      self_size, peer_size, result_size);

  SetJoinResult(ctx, is_left, std::move(join_indices));
  audit::RecordJoinNodeDetail(*ctx, static_cast<int64_t>(self_size),
                              static_cast<int64_t>(peer_size), result_size,
                              start_time);
}

void Join::OprfPsiJoin(ExecContext* ctx, bool is_server,
                       std::optional<util::PsiSizeInfo> psi_size_info) {
  // PsiExecutionInfoTable for audit
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
  auto batch_provider = std::make_shared<util::BatchProvider>(join_keys);

  // set EcdhOprfPsiOptions
  psi::psi::EcdhOprfPsiOptions psi_options;
  auto psi_link = ctx->GetSession()->GetLink();
  if (psi_link->WorldSize() > 2) {
    psi_link = psi_link->SubWorld(ctx->GetNodeName() + "-OprfPsiJoin",
                                  input_party_codes);
  }
  psi_options.link0 = psi_link;
  YACL_ENFORCE(psi_options.link0, "fail to getlink0 for OprfPsiJoin");
  psi_options.link1 = psi_options.link0->Spawn();
  YACL_ENFORCE(psi_options.link1, "fail to getlink1 for OprfPsiJoin");
  psi_options.curve_type =
      static_cast<psi::psi::CurveType>(FLAGS_psi_curve_type);

  // create temp dir
  butil::ScopedTempDir tmp_dir;
  YACL_ENFORCE(tmp_dir.CreateUniqueTempDir(), "fail to create temp dir");

  int64_t join_type = ctx->GetInt64ValueFromAttribute(kJoinTypeAttr);
  JoinRole join_role = GetJoinRole(join_type, is_left);

  if (is_server) {
    OprfPsiServer(ctx, join_role, tmp_dir.path().value(), psi_options,
                  batch_provider, is_left, peer_rank, &psi_info_table,
                  psi_link);
  } else {
    OprfPsiClient(ctx, join_role, tmp_dir.path().value(), psi_options,
                  batch_provider, is_left, peer_rank, &psi_info_table,
                  psi_link);
  }

  // audit
  SPDLOG_INFO(
      "OPRF PSI Join finish, my_party_code:{}, my_rank:{}, total "
      "self_item_count:{}, total peer_item_count:{}, result size:{}",
      ctx->GetSession()->SelfPartyCode(), ctx->GetSession()->SelfRank(),
      psi_info_table.self_size, psi_info_table.peer_size,
      psi_info_table.result_size);
  audit::RecordJoinNodeDetail(
      *ctx, static_cast<int64_t>(psi_info_table.self_size),
      static_cast<int64_t>(psi_info_table.peer_size),
      psi_info_table.result_size, psi_info_table.start_time);
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

void Join::OprfPsiServer(
    ExecContext* ctx, JoinRole join_role, const std::string& tmp_dir,
    const psi::psi::EcdhOprfPsiOptions& psi_options,
    const std::shared_ptr<util::BatchProvider>& batch_provider, bool is_left,
    int64_t peer_rank, util::PsiExecutionInfoTable* psi_info_table,
    std::shared_ptr<yacl::link::Context> psi_link) {
  std::vector<uint8_t> private_key =
      yacl::crypto::SecureRandBytes(psi::psi::kEccKeySize);
  auto ec_oprf_psi_server =
      std::make_shared<psi::psi::EcdhOprfPsiServer>(psi_options, private_key);
  YACL_ENFORCE(ec_oprf_psi_server, "Fail to create EcdhOprfPsiServer");
  auto ub_cache =
      std::make_shared<util::UbPsiJoinCache>(batch_provider->TotalLength());

  auto transfer_server_items_future =
      std::async(std::launch::async, util::OprfPsiServerTransferServerItems,
                 ctx, psi_link, batch_provider, ec_oprf_psi_server, ub_cache);
  util::OprfPsiServerTransferClientItems(ctx, ec_oprf_psi_server);
  transfer_server_items_future.wait();

  uint64_t client_unmatched_count = 0;
  if (join_role == JoinRole::kLeftOrRightJoinNullParty) {
    client_unmatched_count = RecvNullCount(ctx, peer_rank);
  }

  auto matched_seqs = RecvMatchedSeqs(ctx, peer_rank);
  TensorPtr result_tensor =
      BuildServerResult(matched_seqs, ub_cache, client_unmatched_count,
                        join_role, batch_provider);
  psi_info_table->result_size = result_tensor->Length();
  SetJoinResult(ctx, is_left, std::move(result_tensor));
}

void Join::OprfPsiClient(
    ExecContext* ctx, JoinRole join_role, const std::string& tmp_dir,
    const psi::psi::EcdhOprfPsiOptions& psi_options,
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
  transfer_server_items_future.wait();

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
  psi_info_table->result_size = result_tensor->Length();
  SetJoinResult(ctx, is_left, std::move(result_tensor));
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
  auto lctx = ctx->GetSession()->GetLink();
  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, matched_seqs);
  auto tag = ctx->GetNodeName() + "-UbJoinMatchedSeqs";
  lctx->Send(peer_rank, yacl::ByteContainerView(sbuf.data(), sbuf.size()), tag);
}

std::vector<uint64_t> Join::RecvMatchedSeqs(ExecContext* ctx,
                                            int64_t peer_rank) {
  auto lctx = ctx->GetSession()->GetLink();
  auto tag = ctx->GetNodeName() + "-UbJoinMatchedSeqs";
  yacl::Buffer matched_seqs_buf = lctx->Recv(peer_rank, tag);
  msgpack::object_handle oh = msgpack::unpack(
      static_cast<char*>(matched_seqs_buf.data()), matched_seqs_buf.size());
  return oh.get().as<std::vector<uint64_t>>();
}

TensorPtr Join::BuildServerResult(
    const std::vector<uint64_t>& matched_seqs,
    const std::shared_ptr<util::UbPsiJoinCache>& ub_cache,
    uint64_t client_unmatched_count, JoinRole join_role,
    const std::shared_ptr<util::BatchProvider>& batch_provider) {
  UInt64TensorBuilder result_builder;
  result_builder.Reserve(static_cast<int64_t>(matched_seqs.size()));
  std::unordered_set<uint64_t> matched_indices;
  for (uint64_t matched_seq : matched_seqs) {
    uint64_t matched_indice = ub_cache->GetIndice(matched_seq);
    result_builder.UnsafeAppend(matched_indice);
    matched_indices.insert(matched_indice);
  }

  if (join_role == JoinRole::kLeftOrRightJoinNullParty) {
    result_builder.Reserve(static_cast<int64_t>(client_unmatched_count));
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