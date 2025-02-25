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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "psi/algorithm/ecdh/ub_psi/ecdh_oprf_psi.h"
#include "psi/utils/batch_provider.h"
#include "psi/utils/hash_bucket_cache.h"
#include "yacl/link/link.h"

#include "engine/core/tensor.h"
#include "engine/framework/exec.h"
#include "engine/util/psi/batch_provider.h"
#include "engine/util/psi/cipher_intersection.h"
#include "engine/util/stringify_visitor.h"

namespace scql::engine::util {

void OprfPsiServerTransferServerItems(
    ExecContext* ctx, std::shared_ptr<yacl::link::Context> psi_link,
    const std::shared_ptr<BatchProvider>& batch_provider,
    const std::shared_ptr<psi::ecdh::EcdhOprfPsiServer>& dh_oprf_psi_server,
    std::shared_ptr<psi::IUbPsiCache> ub_cache = nullptr);

void OprfPsiServerTransferClientItems(
    ExecContext* ctx,
    const std::shared_ptr<psi::ecdh::EcdhOprfPsiServer>& dh_oprf_psi_server);

void OprfPsiClientTransferServerItems(
    ExecContext* ctx, std::shared_ptr<yacl::link::Context> psi_link,
    const psi::ecdh::EcdhOprfPsiOptions& psi_options,
    const std::shared_ptr<UbPsiCipherStore>& cipher_store);

void OprfPsiClientTransferClientItems(
    ExecContext* ctx,
    const std::shared_ptr<util::BatchProvider>& batch_provider,
    const psi::ecdh::EcdhOprfPsiOptions& psi_options,
    const std::shared_ptr<UbPsiCipherStore>& cipher_store);

void OprfServerTransferShuffledClientItems(
    ExecContext* ctx,
    const std::shared_ptr<psi::ecdh::EcdhOprfPsiServer>& dh_oprf_psi_server,
    const std::string& server_cache_path,
    std::vector<uint64_t>* matched_indices, size_t* self_item_count);

void OprfCLientTransferShuffledClientItems(
    ExecContext* ctx,
    const std::shared_ptr<util::BatchProvider>& batch_provider,
    const psi::ecdh::EcdhOprfPsiOptions& psi_options,
    const std::shared_ptr<UbPsiCipherStore>& client_store,
    const std::shared_ptr<UbPsiCipherStore>& server_store);
}  // namespace scql::engine::util
