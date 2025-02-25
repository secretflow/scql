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

#include "engine/util/psi/cipher_intersection.h"

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/escaping.h"
#include "psi/utils/arrow_csv_batch_provider.h"
#include "psi/utils/batch_provider.h"
#include "psi/utils/hash_bucket_cache.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/primitive_builder.h"
#include "engine/core/tensor_constructor.h"
#include "engine/util/psi/common.h"

DECLARE_int64(provider_batch_size);

namespace scql::engine::util {

void InResultResolver::Set(uint64_t index, bool mask) { masks_[index] = mask; }

TensorPtr InResultResolver::Finalize() {
  arrow::BooleanBuilder mask_builder;
  THROW_IF_ARROW_NOT_OK(mask_builder.AppendValues(masks_));
  std::shared_ptr<arrow::BooleanArray> mask;
  THROW_IF_ARROW_NOT_OK(mask_builder.Finish(&mask));
  arrow::ArrayVector chunks_;
  chunks_.push_back(mask);
  auto result = arrow::ChunkedArray::Make(chunks_, mask->type());
  std::shared_ptr<arrow::ChunkedArray> chunked_arr;
  THROW_IF_ARROW_NOT_OK(std::move(result).Value(&chunked_arr));
  return TensorFrom(chunked_arr);
}

void UbPsiJoinCache::SaveData(yacl::ByteContainerView item, size_t index,
                              size_t shuffle_index) {
  if (idx_ >= seq_to_indice_.size()) {
    YACL_THROW("UbPsiJoin idx out-of-bounds, idx:{} >= vector size:{}", idx_,
               seq_to_indice_.size());
  }
  seq_to_indice_[idx_++] = shuffle_index;
}

UbPsiCipherStore::UbPsiCipherStore(std::string csv_path, bool enable_cache)
    : csv_path_(std::move(csv_path)), enable_cache_(enable_cache) {
  out_ = psi::io::BuildOutputStream(psi::io::FileIoOptions(csv_path_));
  YACL_ENFORCE(out_, "Fail to build outputstream for UbPsiCipherStore");
  out_->Write(kDummyField);
  out_->Write("\n");
}

UbPsiCipherStore::~UbPsiCipherStore() { out_->Close(); }

void UbPsiCipherStore::Save(const std::string& ciphertext,
                            uint32_t duplicate_cnt) {
  auto escaped_ciphertext = absl::Base64Escape(ciphertext);
  for (size_t i = 0; i < duplicate_cnt + 1; ++i) {
    out_->Write(fmt::format("{}\n", escaped_ciphertext));
    if (enable_cache_) {
      data_indices_[escaped_ciphertext].push_back(item_count_);
    }
    ++item_count_;
  }
}

std::vector<std::string> FinalizeAndComputeIntersection(
    const std::shared_ptr<UbPsiCipherStore>& client_store,
    const std::shared_ptr<UbPsiCipherStore>& server_store) {
  client_store->Finalize();
  server_store->Finalize();

  std::vector<std::string> fields{UbPsiCipherStore::kDummyField};
  psi::ArrowCsvBatchProvider server_provider(server_store->GetPath(), fields,
                                             FLAGS_provider_batch_size);

  // may have duplicate items
  std::vector<std::string> results;

  while (true) {
    auto batch_server_data = server_provider.ReadNextBatch();

    if (batch_server_data.empty()) {
      break;
    }

    for (const std::string& server_item : batch_server_data) {
      auto search_ret = client_store->SearchIndices(server_item);
      if (search_ret.has_value()) {
        results.push_back(server_item);
      }
    }
  }

  return results;
}

void InResultResolverWithBucket::FeedBucketData(
    size_t bucket_idx,
    const std::vector<psi::HashBucketCache::BucketItem>& bucket_items,
    const std::vector<uint32_t>& indices,
    const std::unordered_map<std::string, std::vector<size_t>>&
        origin_indices) {
  // build set
  absl::flat_hash_set<size_t> intersection_set;
  for (const auto& indice : indices) {
    intersection_set.emplace(indice);
  }
  std::unique_lock lock(mtx_);
  for (size_t i = 0; i < bucket_items.size(); i++) {
    const auto& bucket_item = bucket_items[i];
    auto it = origin_indices.find(bucket_item.base64_data);
    YACL_ENFORCE(it != origin_indices.end());
    if (intersection_set.contains(i)) {
      for (const auto& idx : it->second) {
        masks_[idx] = true;
      }
    }
  }
}

TensorPtr InResultResolverWithBucket::ComputeInResult() {
  arrow::BooleanBuilder mask_builder;
  THROW_IF_ARROW_NOT_OK(mask_builder.AppendValues(masks_));
  std::shared_ptr<arrow::BooleanArray> mask;
  THROW_IF_ARROW_NOT_OK(mask_builder.Finish(&mask));
  arrow::ArrayVector chunks_;
  chunks_.push_back(mask);
  auto result = arrow::ChunkedArray::Make(chunks_, mask->type());
  std::shared_ptr<arrow::ChunkedArray> chunked_arr;
  THROW_IF_ARROW_NOT_OK(std::move(result).Value(&chunked_arr));
  return TensorFrom(chunked_arr);
}

TensorPtr FinalizeAndComputeOprfInResult(
    const std::shared_ptr<UbPsiCipherStore>& client_store,
    const std::shared_ptr<UbPsiCipherStore>& server_store) {
  client_store->Finalize();
  server_store->Finalize();

  std::unordered_set<uint64_t> client_indices;
  BooleanTensorBuilder result_builder;

  std::vector<std::string> fields{UbPsiCipherStore::kDummyField};
  psi::ArrowCsvBatchProvider server_provider(server_store->GetPath(), fields,
                                             FLAGS_provider_batch_size);

  while (true) {
    auto batch_server_data = server_provider.ReadNextBatch();

    if (batch_server_data.empty()) {
      break;
    }

    for (const std::string& server_item : batch_server_data) {
      auto search_ret = client_store->SearchIndices(server_item);
      if (search_ret.has_value()) {
        auto indices = search_ret.value();
        client_indices.insert(indices.begin(), indices.end());
      }
    }
  }

  result_builder.Reserve(static_cast<int64_t>(client_store->ItemCount()));
  for (uint64_t client_item_idx = 0;
       client_item_idx < client_store->ItemCount(); ++client_item_idx) {
    if (client_indices.count(client_item_idx) > 0) {
      result_builder.UnsafeAppend(true);
    } else {
      result_builder.UnsafeAppend(false);
    }
  }

  TensorPtr result_tensor;
  result_builder.Finish(&result_tensor);
  return result_tensor;
}

std::pair<TensorPtr, std::vector<uint64_t>> FinalizeAndComputeOprfJoinResult(
    const std::shared_ptr<UbPsiCipherStore>& server_store,
    const std::shared_ptr<UbPsiCipherStore>& client_store,
    uint64_t* server_unmatched_count, uint64_t* client_unmatched_count) {
  YACL_ENFORCE(
      server_unmatched_count == nullptr || client_unmatched_count == nullptr,
      "at least one of server_unmatched_count and client_unmatched_count "
      "should "
      "be nullptr");

  server_store->Finalize();
  client_store->Finalize();

  UInt64TensorBuilder result_builder;
  uint64_t server_seq = 0;
  std::vector<uint64_t> matched_seqs;
  std::unordered_set<uint64_t> matched_indices;

  auto add_matched_indice_lambda = [&result_builder, &server_seq, &matched_seqs,
                                    &matched_indices,
                                    client_unmatched_count](uint64_t indice) {
    result_builder.Append(indice);
    matched_seqs.push_back(server_seq);
    if (client_unmatched_count != nullptr) {
      matched_indices.insert(indice);
    }
  };

  std::vector<std::string> fields{UbPsiCipherStore::kDummyField};
  psi::ArrowCsvBatchProvider server_provider(server_store->GetPath(), fields,
                                             FLAGS_provider_batch_size);
  while (true) {
    auto batch_server_data = server_provider.ReadNextBatch();

    if (batch_server_data.empty()) {
      break;
    }

    // TODO(jingshi): search in parallel
    for (const std::string& server_item : batch_server_data) {
      auto search_ret = client_store->SearchIndices(server_item);
      if (search_ret.has_value()) {
        auto indices = search_ret.value();
        std::for_each(indices.begin(), indices.end(),
                      add_matched_indice_lambda);
      } else if (server_unmatched_count != nullptr) {
        ++(*server_unmatched_count);
      }
      ++server_seq;
    }
  }

  if (client_unmatched_count != nullptr) {
    for (uint64_t indice = 0; indice < client_store->ItemCount(); ++indice) {
      if (matched_indices.count(indice) == 0) {
        ++(*client_unmatched_count);
        result_builder.Append(indice);
      }
    }
  }
  if (server_unmatched_count != nullptr) {
    result_builder.Reserve(static_cast<int64_t>(*server_unmatched_count));
    for (uint64_t i = 0; i < *server_unmatched_count; ++i) {
      result_builder.UnsafeAppendNull();
    }
  }

  TensorPtr result_tensor;
  result_builder.Finish(&result_tensor);
  return {result_tensor, matched_seqs};
}

TensorPtr FinalizeAndComputeJoinIndices(
    bool is_left,
    const std::shared_ptr<psi::HashBucketEcPointStore>& self_cache,
    const std::shared_ptr<psi::HashBucketEcPointStore>& peer_cache,
    int64_t join_type) {
  self_cache->Flush();
  peer_cache->Flush();

  if (is_left) {
    return ComputeJoinIndices(self_cache, peer_cache, join_type, is_left);
  } else {
    return ComputeJoinIndices(peer_cache, self_cache, join_type, is_left);
  }
}

TensorPtr ComputeJoinIndices(
    const std::shared_ptr<psi::HashBucketEcPointStore>& left,
    const std::shared_ptr<psi::HashBucketEcPointStore>& right,
    int64_t join_type, bool is_left) {
  YACL_ENFORCE(left->num_bins() == right->num_bins(),
               "left store num_bins={} not equal to right store num_bins={}",
               left->num_bins(), right->num_bins());
  Int64TensorBuilder builder;
  uint64_t outer_join_key_size = 0;
  if (join_type == kLeftJoin) {
    outer_join_key_size = static_cast<uint64_t>(left->ItemCount());
  } else if (join_type == kRightJoin) {
    outer_join_key_size = static_cast<uint64_t>(right->ItemCount());
  } else if (join_type == kInnerJoin) {
    outer_join_key_size = 0;
  }
  absl::flat_hash_set<int64_t> left_matched;
  absl::flat_hash_set<int64_t> right_matched;
  left_matched.reserve(outer_join_key_size);
  right_matched.reserve(outer_join_key_size);
  for (size_t bin_idx = 0; bin_idx < left->num_bins(); ++bin_idx) {
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
      for (auto right_index : right_indices) {
        left_matched.insert(static_cast<int64_t>(left_item.index));
        right_matched.insert(right_index);
        if (is_left) {
          builder.Append(static_cast<int64_t>(left_item.index));
        } else {
          builder.Append(right_index);
        }
      }
    }
  }

  for (uint64_t index = 0; index < outer_join_key_size; index++) {
    if (join_type == kLeftJoin &&
        left_matched.find(index) == left_matched.end()) {
      if (is_left) {
        builder.Append(index);
      } else {
        builder.AppendNull();
      }
    }
    if (join_type == kRightJoin &&
        right_matched.find(index) == right_matched.end()) {
      if (is_left) {
        builder.AppendNull();
      } else {
        builder.Append(index);
      }
    }
  }
  TensorPtr indices;
  builder.Finish(&indices);
  return indices;
}

TensorPtr FinalizeAndComputeInResult(
    bool is_left,
    const std::shared_ptr<psi::HashBucketEcPointStore>& self_cache,
    const std::shared_ptr<psi::HashBucketEcPointStore>& peer_cache) {
  self_cache->Flush();
  peer_cache->Flush();

  if (is_left) {
    return ComputeInResult(self_cache, peer_cache);
  } else {
    return ComputeInResult(peer_cache, self_cache);
  }
}

TensorPtr ComputeInResult(
    const std::shared_ptr<psi::HashBucketEcPointStore>& left,
    const std::shared_ptr<psi::HashBucketEcPointStore>& right) {
  YACL_ENFORCE(left->num_bins() == right->num_bins(),
               "left store num_bins={} not equal to right store num_bins={}",
               left->num_bins(), right->num_bins());
  InResultResolver resolver(left->ItemCount());
  std::mutex mtx;
  yacl::parallel_for(0, left->num_bins(), [&](int64_t begin, int64_t end) {
    for (int64_t bin_idx = begin; bin_idx < end; ++bin_idx) {
      auto left_bucket = left->LoadBucketItems(bin_idx);
      auto right_bucket = right->LoadBucketItems(bin_idx);

      // build set
      absl::flat_hash_set<std::string> right_keys;
      for (const auto& right_item : right_bucket) {
        right_keys.insert(right_item.base64_data);
      }
      std::unique_lock lock(mtx);
      // probe the set
      for (const auto& left_item : left_bucket) {
        resolver.Set(left_item.index,
                     right_keys.contains(left_item.base64_data));
      }
    }
  });
  return resolver.Finalize();
}

}  // namespace scql::engine::util
