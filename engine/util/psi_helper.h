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

#include "libspu/psi/utils/batch_provider.h"
#include "libspu/psi/utils/cipher_store.h"

#include "engine/core/tensor.h"
#include "engine/util/stringify_visitor.h"

namespace scql::engine::util {

/// @brief BatchProvider combines multiple join keys into one
class BatchProvider : public spu::psi::IBatchProvider {
 public:
  explicit BatchProvider(std::vector<TensorPtr> tensors);

  std::vector<std::string> ReadNextBatch(size_t batch_size) override;

 private:
  /// @brief combine two columns into one, just concat them together.
  /// @note current implementation will fail if there exists seperator(like ",")
  /// in columns
  /// @param[in] col1 and @param[in] col2 should have the same length.
  static std::vector<std::string> Combine(const std::vector<std::string>& col1,
                                          const std::vector<std::string>& col2);

  std::vector<TensorPtr> tensors_;

  std::vector<std::unique_ptr<StringifyVisitor>> stringify_visitors_;
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
  TensorPtr FinalizeAndComputeJoinIndices(bool is_left);

 protected:
  /// @param[in] is_left denotes whether to compute the left join indices.
  /// it returns right join indices if is_left == false
  TensorPtr ComputeJoinIndices(spu::psi::HashBucketCache* left,
                               spu::psi::HashBucketCache* right, bool is_left);
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

}  // namespace scql::engine::util