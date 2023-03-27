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

#include "engine/util/psi_helper.h"

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "arrow/compute/api.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/primitive_builder.h"

namespace scql::engine::util {

BatchProvider::BatchProvider(std::vector<TensorPtr> tensors)
    : tensors_(std::move(tensors)) {
  for (size_t i = 0; i < tensors_.size(); ++i) {
    YACL_ENFORCE(tensors_[i]->GetNullCount() == 0,
                 "NULL value is unsupported in PSI");
    if (i > 0 && tensors_[i]->Length() != tensors_[0]->Length()) {
      YACL_THROW("inputs must have the same size");
    }

    stringify_visitors_.push_back(
        std::make_unique<StringifyVisitor>(*tensors_[i]));
  }
}

std::vector<std::string> BatchProvider::ReadNextBatch(size_t batch_size) {
  if (tensors_.size() == 0) {
    return std::vector<std::string>{};
  }

  auto keys = stringify_visitors_[0]->StringifyBatch(batch_size);
  if (keys.empty()) {
    return keys;
  }

  for (size_t i = 1; i < tensors_.size(); ++i) {
    auto another_keys = stringify_visitors_[i]->StringifyBatch(batch_size);

    YACL_ENFORCE(keys.size() == another_keys.size(),
                 "tensor #{} batch size not equals with previous", i);

    keys = Combine(keys, another_keys);
  }

  return keys;
}

std::vector<std::string> BatchProvider::Combine(
    const std::vector<std::string>& col1,
    const std::vector<std::string>& col2) {
  std::vector<std::string> result(col1.size());
  for (size_t i = 0; i < col1.size(); ++i) {
    result[i] = fmt::format("{},{}", col1[i], col2[i]);
  }
  return result;
}

BucketCipherStore::BucketCipherStore(const std::string& disk_cache_dir,
                                     size_t num_bins)
    : num_bins_(std::max(1UL, num_bins)) {
  self_cache_ =
      std::make_unique<spu::psi::HashBucketCache>(disk_cache_dir, num_bins_);
  peer_cache_ =
      std::make_unique<spu::psi::HashBucketCache>(disk_cache_dir, num_bins_);
}

void BucketCipherStore::SaveSelf(std::string ciphertext) {
  self_cache_->WriteItem(ciphertext);
}

void BucketCipherStore::SavePeer(std::string ciphertext) {
  peer_cache_->WriteItem(ciphertext);
}

void BucketCipherStore::Finalize() {
  self_cache_->Flush();
  peer_cache_->Flush();
}

TensorPtr JoinCipherStore::FinalizeAndComputeJoinIndices(bool is_left) {
  Finalize();
  if (is_left) {
    return ComputeJoinIndices(self_cache_.get(), peer_cache_.get(), is_left);
  } else {
    return ComputeJoinIndices(peer_cache_.get(), self_cache_.get(), is_left);
  }
}

TensorPtr JoinCipherStore::ComputeJoinIndices(spu::psi::HashBucketCache* left,
                                              spu::psi::HashBucketCache* right,
                                              bool is_left) {
  Int64TensorBuilder builder;
  for (size_t bin_idx = 0; bin_idx < num_bins_; ++bin_idx) {
    auto left_bucket = left->LoadBucketItems(bin_idx);
    auto right_bucket = right->LoadBucketItems(bin_idx);

    // build hash map
    absl::flat_hash_map<std::string, std::vector<int64_t>>
        right_key_indices_map;
    for (const auto& right_item : right_bucket) {
      auto& indices = right_key_indices_map[right_item.base64_data];
      indices.push_back(static_cast<int64_t>(right_item.index));
    }
    // probe the hash map
    for (const auto& left_item : left_bucket) {
      auto iter = right_key_indices_map.find(left_item.base64_data);
      if (iter == right_key_indices_map.end()) {
        continue;
      }
      const auto& right_indices = iter->second;
      if (is_left) {
        // produce left join indices
        for (size_t i = 0; i < right_indices.size(); ++i) {
          builder.Append(static_cast<int64_t>(left_item.index));
        }
      } else {
        // produce right join indices
        for (size_t i = 0; i < right_indices.size(); ++i) {
          builder.Append(right_indices[i]);
        }
      }
    }
  }
  TensorPtr indices;
  builder.Finish(&indices);
  return indices;
}

TensorPtr InCipherStore::FinalizeAndComputeInResult(bool is_left) {
  Finalize();
  if (is_left) {
    return ComputeInResult(self_cache_.get(), peer_cache_.get());
  } else {
    return ComputeInResult(peer_cache_.get(), self_cache_.get());
  }
}

namespace {

/// @brief restore back the `in` output order via sorting output by index
class InResultResolver {
 public:
  void Append(bool mask, uint64_t index);

  TensorPtr FinalizeAndRestoreResultOrder();

 private:
  arrow::BooleanBuilder mask_builder;
  arrow::UInt64Builder index_builder;
};

void InResultResolver::Append(bool mask, uint64_t index) {
  THROW_IF_ARROW_NOT_OK(mask_builder.Append(mask));
  THROW_IF_ARROW_NOT_OK(index_builder.Append(index));
}

TensorPtr InResultResolver::FinalizeAndRestoreResultOrder() {
  std::shared_ptr<arrow::UInt64Array> index;
  THROW_IF_ARROW_NOT_OK(index_builder.Finish(&index));
  std::shared_ptr<arrow::BooleanArray> mask;
  THROW_IF_ARROW_NOT_OK(mask_builder.Finish(&mask));

  auto sort_indices =
      arrow::compute::SortIndices(*index, arrow::compute::SortOrder::Ascending);
  YACL_ENFORCE(sort_indices.ok(),
               "invoking arrow compute::SortIndices error: {}",
               sort_indices.status().ToString());

  auto result = arrow::compute::Take(*mask, *sort_indices.ValueOrDie());
  YACL_ENFORCE(result.ok(), "invoking arrow compute::Take error: {}",
               result.status().ToString());

  auto chunked_arr = std::make_shared<arrow::ChunkedArray>(result.ValueOrDie());
  return std::make_shared<Tensor>(chunked_arr);
}

}  // namespace

TensorPtr InCipherStore::ComputeInResult(spu::psi::HashBucketCache* left,
                                         spu::psi::HashBucketCache* right) {
  InResultResolver resolver;
  for (size_t bin_idx = 0; bin_idx < num_bins_; ++bin_idx) {
    auto left_bucket = left->LoadBucketItems(bin_idx);
    auto right_bucket = right->LoadBucketItems(bin_idx);

    // build set
    absl::flat_hash_set<std::string> right_keys;
    for (const auto& right_item : right_bucket) {
      right_keys.insert(right_item.base64_data);
    }

    // probe the set
    for (const auto& left_item : left_bucket) {
      if (right_keys.contains(left_item.base64_data)) {
        resolver.Append(true, left_item.index);
      } else {
        resolver.Append(false, left_item.index);
      }
    }
  }
  return resolver.FinalizeAndRestoreResultOrder();
}

BatchFinishedCb::BatchFinishedCb(std::string task_id, size_t batch_total)
    : task_id_(task_id), batch_total_(batch_total) {}

void BatchFinishedCb::operator()(size_t batch_count) {
  if (batch_count % 100 == 0) {
    SPDLOG_INFO(
        "PSI task {} progress report: #{}/{} batches have been completed",
        task_id_, batch_count, batch_total_);
  }
}

}  // namespace scql::engine::util
