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

#include "engine/util/psi/common.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "psi/utils/serialize.h"

namespace scql::engine::util {
PsiPlan GetPsiPlan(int64_t self_length, int64_t peer_length,
                   int64_t unbalance_psi_ratio_threshold,
                   int64_t unbalance_psi_larger_party_rows_count_threshold) {
  util::PsiPlan psi_plan;
  int64_t small_length = std::min(self_length, peer_length);
  int64_t big_length = std::max(self_length, peer_length);
  YACL_ENFORCE(unbalance_psi_ratio_threshold > 1,
               "Invalid unbalance PSI ratio threshold");
  if (small_length > 0 &&
      big_length / small_length >= unbalance_psi_ratio_threshold &&
      big_length > unbalance_psi_larger_party_rows_count_threshold) {
    psi_plan.unbalanced = true;
    // the side with bigger tensor length should be oprf server
    psi_plan.is_server = small_length != self_length;
  }

  psi_plan.psi_size_info.self_size = self_length;
  psi_plan.psi_size_info.peer_size = peer_length;

  return psi_plan;
}

PsiPlan GetOprfPsiPlan(int64_t self_length, int64_t peer_length) {
  util::PsiPlan psi_plan;
  psi_plan.unbalanced = true;
  int64_t small_length = std::min(self_length, peer_length);
  int64_t big_length = std::max(self_length, peer_length);

  YACL_ENFORCE(
      small_length != big_length,
      "When using an unbalanced algorithm, equal data volumes on both "
      "sides indicate a PSI configuration issue. (It is suggested to "
      "set the PSI algorithm to AUTO when the user isn't familiar with PSI)");
  psi_plan.is_server = small_length != self_length;
  psi_plan.psi_size_info.self_size = self_length;
  psi_plan.psi_size_info.peer_size = peer_length;

  return psi_plan;
}

namespace {
constexpr char kPsiInLeft[] = "Left";
constexpr char kPsiInRight[] = "Right";
constexpr char kPsiInputPartyCodesAttr[] = "input_party_codes";
}  // namespace

size_t ExchangeSetSize(const std::shared_ptr<yacl::link::Context>& link_ctx,
                       size_t items_size) {
  YACL_ENFORCE(link_ctx->WorldSize() == 2, "currently only support 2 parties");
  auto result =
      yacl::link::AllGather(link_ctx, psi::utils::SerializeSize(items_size),
                            fmt::format("EXCHANGE_SIZE"));
  return psi::utils::DeserializeSize(result[link_ctx->NextRank()]);
}

PsiPlan CoordinatePsiPlan(ExecContext* ctx, bool force_unbalanced) {
  auto logger = ctx->GetActiveLogger();
  if (force_unbalanced) {
    SPDLOG_LOGGER_INFO(logger,
                       "coordinate between engines to determine OprfPsi plan");
  } else {
    SPDLOG_LOGGER_INFO(logger,
                       "coordinate between engines to determine Psi plan "
                       "(OprfPsi or EcdhPsi, Oprf for unbalanced senario)");
  }
  // get related party codes and ranks
  const auto& my_party_code = ctx->GetSession()->SelfPartyCode();
  std::vector<std::string> input_party_codes =
      ctx->GetStringValuesFromAttribute(kPsiInputPartyCodesAttr);
  bool is_left = my_party_code == input_party_codes.at(0);
  const auto& peer_party_code = input_party_codes.at(is_left ? 1 : 0);
  auto my_rank = ctx->GetSession()->GetPartyRank(my_party_code);
  YACL_ENFORCE(my_rank != -1, "unknown rank for party={}", my_party_code);
  auto peer_rank = ctx->GetSession()->GetPartyRank(peer_party_code);
  YACL_ENFORCE(peer_rank != -1, "unknown rank for party={}", peer_party_code);

  // get input length
  const auto* input_name = is_left ? kPsiInLeft : kPsiInRight;
  const auto& input_pbs = ctx->GetInput(input_name);
  auto* table = ctx->GetTensorTable();
  auto t = table->GetTensor(input_pbs[0].name());
  YACL_ENFORCE(t != nullptr, "tensor {} not found in tensor table",
               input_pbs[0].name());
  int64_t tensor_length = t->Length();
  if (ctx->GetOpType() == "Join") {
    for (int pb_idx = 1; pb_idx < input_pbs.size(); ++pb_idx) {
      auto t = table->GetTensor(input_pbs[pb_idx].name());
      YACL_ENFORCE(t != nullptr, "tensor {} not found in tensor table",
                   input_pbs[pb_idx].name());
      YACL_ENFORCE(tensor_length == t->Length(),
                   "Tensor length in input_pbs should be the same");
    }
  }

  // communicate tensor length
  auto psi_link = ctx->GetSession()->GetLink();
  auto tag = ctx->GetNodeName() + "-TensorLength";
  if (psi_link->WorldSize() > 2) {
    psi_link = psi_link->SubWorld(tag, input_party_codes);
  }
  auto peer_length = ExchangeSetSize(psi_link, tensor_length);

  const auto& session_opts = ctx->GetSession()->GetSessionOptions();

  if (force_unbalanced) {
    return util::GetOprfPsiPlan(tensor_length, peer_length);
  }
  return util::GetPsiPlan(
      tensor_length, peer_length,
      session_opts.psi_config.unbalance_psi_ratio_threshold,
      session_opts.psi_config.unbalance_psi_larger_party_rows_count_threshold);
}

BatchFinishedCb::BatchFinishedCb(std::shared_ptr<spdlog::logger> logger,
                                 std::string task_id, size_t batch_total)
    : task_id_(std::move(task_id)),
      batch_total_(batch_total),
      logger_(std::move(logger)) {}

void BatchFinishedCb::operator()(size_t batch_count) {
  if (batch_count % 100 == 0) {
    SPDLOG_LOGGER_INFO(
        logger_,
        "PSI task {} progress report: #{}/{} batches have been completed",
        task_id_, batch_count, batch_total_);
  }
}

}  // namespace scql::engine::util
