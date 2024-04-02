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

#pragma once

#include <cstdint>
#include <vector>

#include "engine/core/tensor.h"
#include "engine/framework/exec.h"
#include "engine/framework/operator.h"
#include "engine/util/psi_helper.h"

namespace scql::engine::op {

class Join : public Operator {
 public:
  enum class JoinType : int64_t {
    kInnerJoin = 0,
    kLeftJoin = 1,
    kRightJoin = 2,
    kTypeNums,
  };

  enum class JoinRole {
    kInnerJoinParty = 0,
    kLeftOrRightJoinFullParty = 1,
    kLeftOrRightJoinNullParty = 2,
    kInValid
  };

  static const std::string kOpType;
  // input/output names
  static constexpr char kInLeft[] = "Left";
  static constexpr char kInRight[] = "Right";
  static constexpr char kOutLeftJoinIndex[] = "LeftJoinIndex";
  static constexpr char kOutRightJoinIndex[] = "RightJoinIndex";
  // attributes
  static constexpr char kJoinTypeAttr[] = "join_type";
  static constexpr char kInputPartyCodesAttr[] = "input_party_codes";

  static constexpr char kAlgorithmAttr[] = "psi_algorithm";

  static constexpr char kUbPsiServerHint[] = "ub_psi_server_hint";

  const std::string& Type() const override;

 protected:
  void Validate(ExecContext* ctx) override;
  void Execute(ExecContext* ctx) override;

 private:
  static void ValidatePsiVisibility(ExecContext* ctx);
  static void ValidateJoinTypeAndAlgo(ExecContext* ctx);

  static void EcdhPsiJoin(ExecContext* ctx);
  static void OprfPsiJoin(ExecContext* ctx, bool is_server,
                          std::optional<util::PsiSizeInfo> psi_size_info = {});

  static std::vector<TensorPtr> GetJoinKeys(ExecContext* ctx, bool is_left);
  static void SetJoinResult(ExecContext* ctx, bool is_left,
                            TensorPtr result_tensor);
  static JoinRole GetJoinRole(int64_t join_type, bool is_left);

  // oprf psi
  static bool IsOprfServerAccordToHint(ExecContext* ctx);
  static void OprfPsiServer(
      ExecContext* ctx, JoinRole join_role, const std::string& tmp_dir,
      const psi::ecdh::EcdhOprfPsiOptions& psi_options,
      const std::shared_ptr<util::BatchProvider>& batch_provider, bool is_left,
      int64_t peer_rank, util::PsiExecutionInfoTable* psi_info_table,
      std::shared_ptr<yacl::link::Context> psi_link);
  static void OprfPsiClient(
      ExecContext* ctx, JoinRole join_role, const std::string& tmp_dir,
      const psi::ecdh::EcdhOprfPsiOptions& psi_options,
      const std::shared_ptr<util::BatchProvider>& batch_provider, bool is_left,
      int64_t peer_rank, util::PsiExecutionInfoTable* psi_info_table,
      std::shared_ptr<yacl::link::Context> psi_link);

  static uint64_t RecvNullCount(ExecContext* ctx, int64_t peer_rank);
  static void SendNullCount(ExecContext* ctx, int64_t peer_rank,
                            uint64_t null_count);
  static std::vector<uint64_t> RecvMatchedSeqs(ExecContext* ctx,
                                               int64_t peer_rank);
  static void SendMatchedSeqs(ExecContext* ctx, int64_t peer_rank,
                              const std::vector<uint64_t>& matched_seqs);
  static TensorPtr BuildServerResult(
      const std::vector<uint64_t>& matched_seqs,
      const std::shared_ptr<util::UbPsiJoinCache>& ub_cache,
      uint64_t client_unmatched_count, JoinRole join_role,
      const std::shared_ptr<util::BatchProvider>& batch_provider);
};

}  // namespace scql::engine::op