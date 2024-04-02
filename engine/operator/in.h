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
#include <optional>

#include "engine/framework/exec.h"
#include "engine/framework/operator.h"
#include "engine/util/psi_helper.h"

namespace scql::engine::op {

class In : public Operator {
 public:
  enum class InType : int64_t {
    kPsiIn = 0,
    kSecretShareIn = 1,
    kLocalIn = 2,
    kInTypeNums,  // Sentinel Value, must be placed last
  };

  static const std::string kOpType;

  static constexpr char kInLeft[] = "Left";
  static constexpr char kInRight[] = "Right";
  static constexpr char kOut[] = "Out";
  static constexpr char kInType[] = "in_type";
  static constexpr char kAlgorithmAttr[] = "psi_algorithm";
  static constexpr char kInputPartyCodesAttr[] = "input_party_codes";
  static constexpr char kRevealToAttr[] = "reveal_to";

  static constexpr char kUbPsiServerHint[] = "ub_psi_server_hint";

  const std::string& Type() const override;

 protected:
  void Validate(ExecContext* ctx) override;
  void Execute(ExecContext* ctx) override;

 private:
  static void SecretShareIn(ExecContext* ctx);
  static void PsiIn(ExecContext* ctx);
  static void LocalIn(ExecContext* ctx);
  static void EcdhPsiIn(ExecContext* ctx);
  static void OprfPsiIn(ExecContext* ctx, bool is_server,
                        std::optional<util::PsiSizeInfo> = {});

  static void ValidateInputAndOutputForPsi(ExecContext* ctx);
  static void ValidatePartyCodesForPsi(ExecContext* ctx);

  // oprf psi
  static bool IsOprfServerAccordToHint(ExecContext* ctx);
  static void OprfPsiServer(
      ExecContext* ctx, bool reveal_to_server, const std::string& tmp_dir,
      const psi::ecdh::EcdhOprfPsiOptions& psi_options,
      const std::shared_ptr<util::BatchProvider>& batch_provider,
      util::PsiExecutionInfoTable* psi_info_table,
      std::shared_ptr<yacl::link::Context> psi_link);
  static void OprfPsiClient(
      ExecContext* ctx, bool reveal_to_server, const std::string& tmp_dir,
      const psi::ecdh::EcdhOprfPsiOptions& psi_options,
      const std::shared_ptr<util::BatchProvider>& batch_provider,
      util::PsiExecutionInfoTable* psi_info_table,
      std::shared_ptr<yacl::link::Context> psi_link);

  static int64_t OprfServerHandleResult(
      ExecContext* ctx, const std::vector<uint64_t>& matched_indices,
      size_t self_item_count);
  static int64_t OprfClientHandleResult(
      ExecContext* ctx,
      const std::shared_ptr<util::UbPsiCipherStore>& client_store,
      const std::shared_ptr<util::UbPsiCipherStore>& server_store);
};

}  // namespace scql::engine::op