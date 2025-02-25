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
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "psi/utils/batch_provider.h"
#include "psi/utils/hash_bucket_cache.h"
#include "yacl/link/link.h"

#include "engine/core/tensor.h"
#include "engine/core/tensor_slice.h"
#include "engine/util/stringify_visitor.h"

namespace scql::engine::util {

using scql::engine::TensorPtr;
using scql::engine::util::StringifyVisitor;

/// @brief BatchProvider combines multiple join keys into one
class BatchProvider : public ::psi::IBasicBatchProvider,
                      public ::psi::IShuffledBatchProvider {
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

class BucketProvider {
 public:
  using DataPair = std::pair<std::string, size_t>;
  explicit BucketProvider(const std::vector<TensorPtr>& tensors)
      : tensors_(tensors) {}
  void InitBucket(const std::shared_ptr<yacl::link::Context>& lctx,
                  size_t self_size, size_t peer_size);
  std::vector<DataPair> GetBucketIdx(size_t idx);
  std::vector<psi::HashBucketCache::BucketItem> GetDeDupItemsInBucket(
      size_t idx);
  size_t GetBucketNum() const { return bucket_num_; }
  bool IsBucketTensor() const { return is_bucket_tensor_; }
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
  std::vector<TensorPtr> tensors_;
  std::vector<std::vector<DataPair>> bucket_items_;
  size_t item_index_ = 0;
  // bucket_dup_idx_[i][j] means base64 data to indices of the i-th bucket
  std::vector<std::unordered_map<std::string, std::vector<size_t>>>
      bucket_dup_idx_;
  std::vector<std::shared_ptr<TensorSlice>> tensor_slices_;
  bool is_bucket_tensor_ = true;
  size_t bucket_num_ = 0;
  static constexpr size_t kBucketSize = 1000 * 1000;
};

/// @brief combine two columns into one, just concat them together.
/// @note current implementation will fail if there exists separator(like
/// ",") in columns
/// @param[in] col1 and @param[in] col2 should have the same length.
std::vector<std::string> Combine(const std::vector<std::string>& col1,
                                 const std::vector<std::string>& col2);

}  // namespace scql::engine::util
