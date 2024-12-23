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
#include <unordered_map>
#include <vector>

#include "gflags/gflags.h"
#include "psi/ecdh/ub_psi/ecdh_oprf_psi.h"
#include "psi/utils/batch_provider.h"
#include "psi/utils/ec_point_store.h"
#include "psi/utils/hash_bucket_cache.h"
#include "psi/utils/ub_psi_cache.h"
#include "spdlog/spdlog.h"

#include "engine/core/tensor.h"
#include "engine/framework/exec.h"
#include "engine/util/stringify_visitor.h"

DECLARE_int64(provider_batch_size);

namespace scql::engine::util {

static constexpr int64_t kInnerJoin = 0;
static constexpr int64_t kLeftJoin = 1;
static constexpr int64_t kRightJoin = 2;
static constexpr size_t kNumBins = 64;

enum class PsiAlgo : int64_t {
  kAutoPsi = 0,
  kEcdhPsi = 1,
  kOprfPsi = 2,
  kRr22Psi = 3,
  kAlgoNums,  // Sentinel Value
};

struct PsiSizeInfo {
  size_t self_size = 0;
  size_t peer_size = 0;
};

struct PsiPlan {
  bool unbalanced = false;
  bool is_server = false;
  PsiSizeInfo psi_size_info;
};

struct PsiExecutionInfoTable {
  decltype(std::chrono::system_clock::now()) start_time;
  size_t self_size;
  size_t peer_size;
  int64_t result_size;
};

PsiPlan GetPsiPlan(int64_t self_length, int64_t peer_length,
                   int64_t unbalance_psi_ratio_threshold,
                   int64_t unbalance_psi_larger_party_rows_count_threshold);

PsiPlan CoordinatePsiPlan(ExecContext* ctx);

/// @brief combine two columns into one, just concat them together.
/// @note current implementation will fail if there exists separator(like
/// ",") in columns
/// @param[in] col1 and @param[in] col2 should have the same length.
std::vector<std::string> Combine(const std::vector<std::string>& col1,
                                 const std::vector<std::string>& col2);

/// @brief BatchProvider combines multiple join keys into one
class BatchProvider : public psi::IBasicBatchProvider,
                      public psi::IShuffledBatchProvider {
 public:
  explicit BatchProvider(std::vector<TensorPtr> tensors,
                         size_t batch_size = 8192);

  std::vector<std::string> ReadNextBatch() override;
  psi::IShuffledBatchProvider::ShuffledBatch ReadNextShuffledBatch() override;

  int64_t TotalLength() const {
    return tensors_.empty() ? 0 : tensors_[0]->Length();
  }

  size_t batch_size() const override { return batch_size_; }

 private:
  std::vector<TensorPtr> tensors_;
  std::vector<std::unique_ptr<StringifyVisitor>> stringify_visitors_;
  size_t idx_;
  size_t batch_size_;
};

// param is_left represents whether myself on the left side of join
TensorPtr FinalizeAndComputeJoinIndices(
    bool is_left,
    const std::shared_ptr<psi::HashBucketEcPointStore>& self_cache,
    const std::shared_ptr<psi::HashBucketEcPointStore>& peer_cache,
    int64_t join_type);

/// @param[in] is_left denotes whether to compute the left join indices.
/// it returns right join indices if is_left == false
TensorPtr ComputeJoinIndices(
    const std::shared_ptr<psi::HashBucketEcPointStore>& left,
    const std::shared_ptr<psi::HashBucketEcPointStore>& right,
    int64_t join_type, bool is_left);

/// @param[in] is_left represents whether myself on the left side of in
TensorPtr FinalizeAndComputeInResult(
    bool is_left,
    const std::shared_ptr<psi::HashBucketEcPointStore>& self_cache,
    const std::shared_ptr<psi::HashBucketEcPointStore>& peer_cache);

TensorPtr ComputeInResult(
    const std::shared_ptr<psi::HashBucketEcPointStore>& left,
    const std::shared_ptr<psi::HashBucketEcPointStore>& right);

class BatchFinishedCb {
 public:
  BatchFinishedCb(std::shared_ptr<spdlog::logger> logger, std::string task_id,
                  size_t batch_total);

  void operator()(size_t batch_count);

 private:
  const std::string task_id_;
  size_t batch_total_;
  std::shared_ptr<spdlog::logger> logger_;
};

class UbPsiCipherStore : public psi::IEcPointStore {
 public:
  explicit UbPsiCipherStore(std::string csv_path, bool enable_cache);

  ~UbPsiCipherStore() override;

  void Save(const std::string& ciphertext, uint32_t duplicate_cnt) override;

  void Flush() override { out_->Flush(); }

  uint64_t ItemCount() override { return item_count_; }

  void Finalize() { return Flush(); }

  std::string GetPath() const { return csv_path_; }

  std::optional<std::vector<size_t>> SearchIndices(
      const std::string& ciphertext) {
    auto iter = data_indices_.find(ciphertext);
    if (iter != data_indices_.end()) {
      return iter->second;
    } else {
      return {};
    }
  }

  static inline const char* kDummyField = "evaluated_items";

 protected:
  std::string csv_path_;

  std::unordered_map<std::string, std::vector<size_t>> data_indices_;
  size_t item_count_ = 0;

 private:
  bool enable_cache_;
  std::unique_ptr<psi::io::OutputStream> out_;
};

class UbPsiJoinCache : public psi::IUbPsiCache {
 public:
  explicit UbPsiJoinCache(size_t size) : seq_to_indice_(size) {}
  void SaveData(yacl::ByteContainerView item, size_t index,
                size_t shuffle_index) override;

  size_t GetIndice(size_t seq) { return seq_to_indice_.at(seq); }

 private:
  size_t idx_ = 0;
  std::vector<size_t> seq_to_indice_;
};

class MemoryBucketProvider {
 public:
  using DataPair = std::pair<std::string, size_t>;
  explicit MemoryBucketProvider(
      const std::shared_ptr<util::BatchProvider>& batch_provider)
      : batch_provider_(batch_provider) {}
  void InitBucket(const std::shared_ptr<yacl::link::Context>& lctx,
                  size_t self_size, size_t peer_size);
  std::vector<DataPair> GetBucketIdx(size_t idx);
  std::vector<psi::HashBucketCache::BucketItem> GetDeDupItemsInBucket(
      size_t idx);
  size_t GetBucketNum() { return bucket_items_.size(); }
  TensorPtr CalIntersection(
      const std::shared_ptr<yacl::link::Context>& lctx, size_t bucket_idx,
      bool is_left, int64_t join_type,
      const std::vector<psi::HashBucketCache::BucketItem>& bucket_items,
      const std::vector<uint32_t>& indices,
      const std::vector<uint32_t>& peer_cnt);

  std::unordered_map<std::string, std::vector<size_t>> GetDupIndices(
      size_t bucket_idx) {
    return bucket_dup_idx_[bucket_idx];
  }

  void CleanBucket(size_t idx) {
    bucket_items_[idx].clear();
    bucket_dup_idx_[idx].clear();
  }

 private:
  std::shared_ptr<util::BatchProvider> batch_provider_;
  std::vector<std::vector<DataPair>> bucket_items_;
  size_t item_index_ = 0;
  // bucket_dup_idx_[i][j] means base64 data to indices of the i-th bucket
  std::vector<std::unordered_map<std::string, std::vector<size_t>>>
      bucket_dup_idx_;
  static constexpr size_t kBucketSize = 1000 * 1000;
};

class InResultResolverWithBucket {
 public:
  explicit InResultResolverWithBucket(size_t bucket_num) {
    mask_arrays_.resize(bucket_num);
    index_arrays_.resize(bucket_num);
  }

  void FeedBucketData(
      size_t bucket_idx,
      const std::vector<psi::HashBucketCache::BucketItem>& bucket_items,
      const std::vector<uint32_t>& indices,
      const std::unordered_map<std::string, std::vector<size_t>>&
          origin_indices);

  TensorPtr ComputeInResult();

 private:
  std::vector<std::shared_ptr<arrow::BooleanArray>> mask_arrays_;
  std::vector<std::shared_ptr<arrow::UInt64Array>> index_arrays_;
};

TensorPtr FinalizeAndCompute22InResult(
    const std::shared_ptr<UbPsiCipherStore>& client_store,
    const std::shared_ptr<UbPsiCipherStore>& server_store);

TensorPtr FinalizeAndComputeOprfInResult(
    const std::shared_ptr<UbPsiCipherStore>& client_store,
    const std::shared_ptr<UbPsiCipherStore>& server_store);

std::pair<TensorPtr, std::vector<uint64_t>> FinalizeAndComputeOprfJoinResult(
    const std::shared_ptr<UbPsiCipherStore>& server_store,
    const std::shared_ptr<UbPsiCipherStore>& client_store,
    uint64_t* server_unmatched_count, uint64_t* client_unmatched_count);

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

size_t ExchangeSetSize(const std::shared_ptr<yacl::link::Context>& link_ctx,
                       size_t items_size);

TensorPtr ConcatTensors(const std::vector<TensorPtr>& tensors);

/// @brief restore back the `in` output order via sorting output by index
class InResultResolver {
 public:
  void Append(bool mask, uint64_t index);

  TensorPtr FinalizeAndRestoreResultOrder();

 private:
  arrow::BooleanBuilder mask_builder_;
  arrow::UInt64Builder index_builder_;
};
}  // namespace scql::engine::util
