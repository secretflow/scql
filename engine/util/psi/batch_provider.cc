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

#include "engine/util/psi/batch_provider.h"

#include <cstddef>

#include "absl/strings/escaping.h"
#include "gflags/gflags.h"
#include "yacl/crypto/rand/rand.h"

#include "engine/core/primitive_builder.h"
#include "engine/core/tensor.h"
#include "engine/util/psi/common.h"
#include "engine/util/tensor_util.h"

DEFINE_int64(provider_batch_size, 8192, "batch size used in PSI Provider");

namespace scql::engine::util {

std::vector<std::string> Combine(const std::vector<std::string>& col1,
                                 const std::vector<std::string>& col2) {
  std::vector<std::string> result(col1.size());
  for (size_t i = 0; i < col1.size(); ++i) {
    result[i] = fmt::format("{},{}", col1[i], col2[i]);
  }
  return result;
}

BatchProvider::BatchProvider(std::vector<TensorPtr> tensors, size_t batch_size)
    : tensors_(std::move(tensors)), idx_(0), batch_size_(batch_size) {
  for (size_t i = 0; i < tensors_.size(); ++i) {
    YACL_ENFORCE(tensors_[i]->GetNullCount() == 0,
                 "NULL value is unsupported in PSI");
    if (i > 0 && tensors_[i]->Length() != tensors_[0]->Length()) {
      YACL_THROW("inputs must have the same size");
    }

    stringify_visitors_.push_back(
        std::make_unique<StringifyVisitor>(tensors_[i], batch_size_));
  }
}

std::vector<std::string> BatchProvider::ReadNextBatch() {
  if (tensors_.empty()) {
    return std::vector<std::string>{};
  }

  auto keys = stringify_visitors_[0]->StringifyBatch();
  if (keys.empty()) {
    return keys;
  }

  for (size_t i = 1; i < tensors_.size(); ++i) {
    auto another_keys = stringify_visitors_[i]->StringifyBatch();
    YACL_ENFORCE(keys.size() == another_keys.size(),
                 "tensor #{} batch size not equals with previous", i);

    keys = Combine(keys, another_keys);
  }

  return keys;
}

psi::IShuffledBatchProvider::ShuffledBatch
BatchProvider::ReadNextShuffledBatch() {
  std::vector<size_t> batch_indices;
  std::vector<size_t> shuffle_indices;
  if (tensors_.empty()) {
    std::vector<std::string> keys;
    return {keys, batch_indices, shuffle_indices, {}};
  }

  auto keys = ReadNextBatch();
  batch_indices.resize(keys.size());
  std::iota(batch_indices.begin(), batch_indices.end(), idx_);

  shuffle_indices.resize(keys.size());
  std::iota(shuffle_indices.begin(), shuffle_indices.end(), 0);
  std::mt19937 rng(yacl::crypto::SecureRandU64());
  std::shuffle(shuffle_indices.begin(), shuffle_indices.end(), rng);
  std::vector<std::string> shuffled_keys(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    shuffled_keys[i].swap(keys[shuffle_indices[i]]);
  }
  keys.swap(shuffled_keys);
  for (size_t i = 0; i < keys.size(); ++i) {
    shuffle_indices[i] += idx_;
  }

  idx_ += keys.size();

  return {keys, batch_indices, shuffle_indices,
          std::vector<uint32_t>(keys.size(), 0)};
}

void BucketProvider::InitBucket(
    const std::shared_ptr<yacl::link::Context>& lctx, size_t self_size,
    size_t peer_size) {
  is_bucket_tensor_ = AreAllBucketTensor(tensors_);
  if (is_bucket_tensor_) {
    for (auto& tensor : tensors_) {
      tensor_slices_.push_back(CreateTensorSlice(tensor));
    }
    bucket_num_ = tensor_slices_[0]->GetSliceNum();
    bucket_items_.resize(bucket_num_);
    bucket_dup_idx_.resize(bucket_num_);
    return;
  }
  auto bucket_size =
      std::min(kBucketSize, util::ExchangeSetSize(lctx, kBucketSize));
  size_t bucket_num =
      (std::max(self_size, peer_size) + bucket_size - 1) / bucket_size;
  bucket_num = std::max(bucket_num, static_cast<size_t>(1));
  bucket_items_.resize(bucket_num);
  bucket_dup_idx_.resize(bucket_num);
  BatchProvider batch_provider(tensors_);
  while (true) {
    auto inputs = batch_provider.ReadNextBatch();
    if (inputs.empty()) {
      break;
    }
    for (auto& in_str : inputs) {
      auto base64_data = absl::Base64Escape(in_str);
      bucket_items_[std::hash<std::string>()(base64_data) % bucket_num]
          .emplace_back(base64_data, item_index_);
      item_index_++;
    }
  }
  bucket_num_ = bucket_items_.size();
}

std::vector<BucketProvider::DataPair> BucketProvider::GetBucketIdx(size_t idx) {
  YACL_ENFORCE(idx < bucket_num_);
  if (!is_bucket_tensor_) {
    return bucket_items_[idx];
  }
  std::vector<TensorPtr> bucket_ts(tensor_slices_.size());
  for (size_t i = 0; i < bucket_ts.size(); i++) {
    auto t = tensor_slices_[i]->GetSlice(idx);
    if (t == nullptr) {
      break;
    }
    bucket_ts[i] = t;
  }
  std::vector<DataPair> bucket_items;
  BatchProvider batch_provider(bucket_ts);
  size_t item_index_ = 0;
  while (true) {
    auto inputs = batch_provider.ReadNextBatch();
    if (inputs.empty()) {
      break;
    }
    for (auto& in_str : inputs) {
      auto base64_data = absl::Base64Escape(in_str);
      bucket_items.emplace_back(base64_data, item_index_);
      item_index_++;
    }
  }
  return bucket_items;
}

std::vector<psi::HashBucketCache::BucketItem>
BucketProvider::GetDeDupItemsInBucket(size_t idx) {
  auto items = GetBucketIdx(idx);
  if (items.size() == 0) {
    return {};
  }
  std::vector<psi::HashBucketCache::BucketItem> bucket_items;
  std::unordered_map<std::string, std::vector<size_t>> cur_bucket_dup_idx;
  for (auto& item : items) {
    const auto& iter = cur_bucket_dup_idx.find(item.first);
    if (iter != cur_bucket_dup_idx.end()) {
      iter->second.push_back(item.second);
    } else {
      cur_bucket_dup_idx.insert({item.first, {item.second}});
      psi::HashBucketCache::BucketItem bucket_item;
      bucket_item.index = bucket_items.size();
      bucket_item.extra_dup_cnt = 0;
      bucket_item.base64_data = item.first;
      bucket_items.push_back(bucket_item);
    }
  }
  for (auto& bucket_item : bucket_items) {
    bucket_item.extra_dup_cnt =
        cur_bucket_dup_idx[bucket_item.base64_data].size() - 1;
  }
  bucket_dup_idx_[idx] = std::move(cur_bucket_dup_idx);
  return bucket_items;
}

TensorPtr BucketProvider::CalIntersection(
    const std::shared_ptr<yacl::link::Context>& lctx, size_t bucket_idx,
    bool is_left, int64_t join_type,
    const std::vector<psi::HashBucketCache::BucketItem>& bucket_items,
    const std::vector<uint32_t>& indices,
    const std::vector<uint32_t>& peer_cnt) {
  YACL_ENFORCE(bucket_idx < bucket_num_);
  UInt64TensorBuilder builder;
  TensorPtr t;
  auto bucket_dup_idx = bucket_dup_idx_[bucket_idx];
  for (size_t i = 0; i < indices.size(); i++) {
    const auto& bucket_item = bucket_items[indices[i]];
    auto dup_idx = bucket_dup_idx[bucket_item.base64_data];
    if (is_left) {
      for (size_t k = 0; k < peer_cnt[i] + 1; k++) {
        for (size_t j = 0; j < dup_idx.size(); j++) {
          builder.Append(dup_idx[j]);
        }
      }
    } else {
      for (size_t j = 0; j < dup_idx.size(); j++) {
        for (size_t k = 0; k < peer_cnt[i] + 1; k++) {
          builder.Append(dup_idx[j]);
        }
      }
    }
  }
  if ((join_type == kLeftJoin && is_left) ||
      (join_type == kRightJoin && !is_left)) {
    size_t null_cnt = 0;
    std::unordered_set<uint32_t> indices_set(indices.size());
    for (const auto indice : indices) {
      indices_set.insert(indice);
    }
    for (size_t i = 0; i < bucket_items.size(); i++) {
      if (indices_set.find(i) == indices_set.end()) {
        for (const auto idx : bucket_dup_idx[bucket_items[i].base64_data]) {
          builder.Append(idx);
          null_cnt++;
        }
      }
    }
    lctx->Send(lctx->NextRank(),
               yacl::ByteContainerView(&null_cnt, sizeof(null_cnt)),
               "null count");
  } else if (join_type == kLeftJoin || join_type == kRightJoin) {
    auto data = lctx->Recv(lctx->NextRank(), "null count");
    auto* null_cnt = data.data<size_t>();
    for (size_t i = 0; i < *null_cnt; i++) {
      builder.AppendNull();
    }
  }
  builder.Finish(&t);
  return t;
}

}  // namespace scql::engine::util
