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

#include "psi/utils/batch_provider.h"
#include "psi/utils/ec_point_store.h"
#include "psi/utils/hash_bucket_cache.h"
#include "psi/utils/ub_psi_cache.h"
#include "yacl/link/link.h"

#include "engine/core/tensor.h"

namespace scql::engine::util {

class InResultResolver {
 public:
  explicit InResultResolver(size_t result_size) { masks_.resize(result_size); }
  void Set(uint64_t index, bool mask);

  TensorPtr Finalize();

 private:
  std::vector<bool> masks_;
};

class InResultResolverWithBucket {
 public:
  explicit InResultResolverWithBucket(size_t result_size) {
    masks_.resize(result_size, false);
  }

  void FeedBucketData(
      size_t bucket_idx,
      const std::vector<psi::HashBucketCache::BucketItem>& bucket_items,
      const std::vector<uint32_t>& indices,
      const std::unordered_map<std::string, std::vector<size_t>>&
          origin_indices);

  TensorPtr ComputeInResult();

 private:
  std::vector<bool> masks_;
  std::mutex mtx_;
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

TensorPtr FinalizeAndComputeOprfInResult(
    const std::shared_ptr<UbPsiCipherStore>& client_store,
    const std::shared_ptr<UbPsiCipherStore>& server_store);

std::pair<TensorPtr, std::vector<uint64_t>> FinalizeAndComputeOprfJoinResult(
    const std::shared_ptr<UbPsiCipherStore>& server_store,
    const std::shared_ptr<UbPsiCipherStore>& client_store,
    uint64_t* server_unmatched_count, uint64_t* client_unmatched_count);

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

std::vector<std::string> FinalizeAndComputeIntersection(
    const std::shared_ptr<UbPsiCipherStore>& client_store,
    const std::shared_ptr<UbPsiCipherStore>& server_store);

}  // namespace scql::engine::util