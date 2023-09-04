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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "libspu/psi/core/ecdh_oprf_psi.h"
#include "libspu/psi/io/io.h"
#include "libspu/psi/utils/batch_provider.h"
#include "libspu/psi/utils/cipher_store.h"
#include "libspu/psi/utils/ub_psi_cache.h"

#include "engine/core/tensor.h"
#include "engine/framework/exec.h"
#include "engine/util/stringify_visitor.h"

namespace scql::engine::util {

static constexpr int64_t kInnerJoin = 0;
static constexpr int64_t kLeftJoin = 1;
static constexpr int64_t kRightJoin = 2;

struct PsiSizeInfo {
  size_t self_size;
  size_t peer_size;
};

struct PsiPlan {
  bool unbalanced = false;
  bool is_server;
  PsiSizeInfo psi_size_info;
};

struct PsiExecutionInfoTable {
  decltype(std::chrono::system_clock::now()) start_time;
  size_t self_size;
  size_t peer_size;
  int64_t result_size;
};

PsiPlan GetPsiPlan(int64_t self_length, int64_t peer_length);

PsiPlan CoordinatePsiPlan(ExecContext* ctx);

/// @brief combine two columns into one, just concat them together.
/// @note current implementation will fail if there exists separator(like
/// ",") in columns
/// @param[in] col1 and @param[in] col2 should have the same length.
std::vector<std::string> Combine(const std::vector<std::string>& col1,
                                 const std::vector<std::string>& col2);

/// @brief BatchProvider combines multiple join keys into one
class BatchProvider : public spu::psi::IBatchProvider,
                      public spu::psi::IShuffleBatchProvider {
 public:
  explicit BatchProvider(std::vector<TensorPtr> tensors, bool shuffle = false);

  std::vector<std::string> ReadNextBatch(size_t batch_size) override;

  std::tuple<std::vector<std::string>, std::vector<size_t>, std::vector<size_t>>
  ReadNextBatchWithIndex(size_t batch_size) override;

  int64_t TotalLength() const {
    return tensors_.empty() ? 0 : tensors_[0]->Length();
  }

 private:
  std::vector<TensorPtr> tensors_;

  std::vector<std::unique_ptr<StringifyVisitor>> stringify_visitors_;

  size_t idx_;

  bool shuffle_;
};

class BucketCipherStore : public spu::psi::ICipherStore {
 public:
  explicit BucketCipherStore(const std::string& disk_cache_dir,
                             size_t num_bins);

  void SaveSelf(std::string ciphertext) override;
  void SavePeer(std::string ciphertext) override;

  size_t GetSelfItemCount() const { return self_cache_->ItemCount(); }
  size_t GetPeerItemCount() const { return peer_cache_->ItemCount(); }

 protected:
  void Finalize();

 protected:
  const size_t num_bins_;

  std::unique_ptr<spu::psi::HashBucketCache> self_cache_;
  std::unique_ptr<spu::psi::HashBucketCache> peer_cache_;
};

class JoinCipherStore : public BucketCipherStore {
 public:
  using BucketCipherStore::BucketCipherStore;
  // param is_left represents whether myself on the left side of join
  TensorPtr FinalizeAndComputeJoinIndices(int64_t join_type, bool is_left);

 protected:
  /// @param[in] is_left denotes whether to compute the left join indices.
  /// it returns right join indices if is_left == false
  TensorPtr ComputeJoinIndices(spu::psi::HashBucketCache* left,
                               spu::psi::HashBucketCache* right,
                               int64_t join_type, bool is_left);
};

class InCipherStore : public BucketCipherStore {
 public:
  using BucketCipherStore::BucketCipherStore;

  /// @param[in] is_left represents whether myself on the left side of in
  TensorPtr FinalizeAndComputeInResult(bool is_left);

 protected:
  TensorPtr ComputeInResult(spu::psi::HashBucketCache* left,
                            spu::psi::HashBucketCache* right);
};

class BatchFinishedCb {
 public:
  BatchFinishedCb(std::string task_id, size_t batch_total);

  void operator()(size_t batch_count);

 private:
  const std::string task_id_;
  size_t batch_total_;
};

class UbOprfPsiClientCipherStore : public spu::psi::ICipherStore {
 public:
  explicit UbOprfPsiClientCipherStore(std::string server_csv_path,
                                      bool to_write_server = true);

  ~UbOprfPsiClientCipherStore() override;

  void SaveSelf(std::string ciphertext) override;

  void SavePeer(std::string ciphertext) override;

  size_t GetClientItemCount() const { return client_item_count_; }
  size_t GetServerItemCount() const { return server_item_count_; }

  void Finalize();
  std::vector<std::string> ComputeIntersection();
  std::vector<std::string> FinalizeAndComputeIntersection();

 protected:
  static inline const char* kDummyField = "evaluated_items";
  static constexpr size_t kBatchSize =
      8192;  // same with spu::psi::EcdhOprfPsiOptions
  std::string server_csv_path_;

  std::unordered_map<std::string, std::vector<size_t>> client_data_;
  size_t client_item_count_ = 0;
  size_t server_item_count_ = 0;

 private:
  static constexpr size_t kLogInterval = 10000;
  bool to_write_server_;
  std::unique_ptr<spu::psi::io::OutputStream> server_out_;
};

class UbInCipherStore : public UbOprfPsiClientCipherStore {
 public:
  using UbOprfPsiClientCipherStore::UbOprfPsiClientCipherStore;

  TensorPtr FinalizeAndComputeInResult();

 private:
  TensorPtr ComputeInResult();
};

class UbJoinCipherStore : public UbOprfPsiClientCipherStore {
 public:
  using UbOprfPsiClientCipherStore::UbOprfPsiClientCipherStore;

  std::pair<TensorPtr, std::vector<uint64_t>> FinalizeAndComputeJoinResult(
      uint64_t* server_unmatched_count = nullptr,
      uint64_t* client_unmatched_count = nullptr);

  std::pair<TensorPtr, std::vector<uint64_t>> ComputeJoinResult(
      uint64_t* server_unmatched_count = nullptr,
      uint64_t* client_unmatched_count = nullptr);
};

class UbPsiJoinCache : public spu::psi::IUbPsiCache {
 public:
  explicit UbPsiJoinCache(size_t size) : seq_to_indice_(size){};
  void SaveData(yacl::ByteContainerView item, size_t index,
                size_t shuffle_index) override;

  size_t GetIndice(size_t seq) { return seq_to_indice_.at(seq); }

 private:
  std::vector<size_t> seq_to_indice_;
};

void OprfPsiServerTransferServerItems(
    ExecContext* ctx, const std::shared_ptr<BatchProvider>& batch_provider,
    const std::shared_ptr<spu::psi::EcdhOprfPsiServer>& dh_oprf_psi_server,
    std::shared_ptr<spu::psi::IUbPsiCache> ub_cache = nullptr);

void OprfPsiServerTransferClientItems(
    ExecContext* ctx,
    const std::shared_ptr<spu::psi::EcdhOprfPsiServer>& dh_oprf_psi_server);

void OprfPsiClientTransferServerItems(
    ExecContext* ctx, const spu::psi::EcdhOprfPsiOptions& psi_options,
    const std::shared_ptr<util::UbOprfPsiClientCipherStore>& cipher_store);

void OprfPsiClientTransferClientItems(
    ExecContext* ctx,
    const std::shared_ptr<util::BatchProvider>& batch_provider,
    const spu::psi::EcdhOprfPsiOptions& psi_options,
    const std::shared_ptr<util::UbOprfPsiClientCipherStore>& cipher_store);

void OprfServerTransferShuffledClientItems(
    ExecContext* ctx,
    const std::shared_ptr<spu::psi::EcdhOprfPsiServer>& dh_oprf_psi_server,
    const std::string& server_cache_path, size_t batch_size,
    std::vector<uint64_t>* matched_indices, size_t* self_item_count);

void OprfCLientTransferShuffledClientItems(
    ExecContext* ctx,
    const std::shared_ptr<util::BatchProvider>& batch_provider,
    const spu::psi::EcdhOprfPsiOptions& psi_options,
    const std::shared_ptr<util::UbInCipherStore>& cipher_store);

}  // namespace scql::engine::util
