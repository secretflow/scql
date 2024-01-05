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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <tuple>
#include <unordered_set>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "arrow/compute/api.h"
#include "gflags/gflags.h"
#include "msgpack.hpp"
#include "yacl/crypto/utils/rand.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/primitive_builder.h"
#include "engine/core/tensor.h"

DEFINE_int64(unbalance_psi_ratio_threshold, 5,
             "minimum LargePartySize/SmallPartySize ratio to choose unbalanced "
             "PSI, LargePartySize means the rows count of the larger party");
DEFINE_int64(unbalance_psi_larger_party_rows_count_threshold, 81920,
             "minimum rows count of the larger party to choose unbalanced PSI");
DEFINE_int32(psi_curve_type, psi::psi::CURVE_FOURQ, "curve type used in PSI");

namespace scql::engine::util {
PsiPlan GetPsiPlan(int64_t self_length, int64_t peer_length) {
  util::PsiPlan psi_plan;
  int64_t small_length = std::min(self_length, peer_length);
  int64_t big_length = std::max(self_length, peer_length);
  YACL_ENFORCE(FLAGS_unbalance_psi_ratio_threshold >= 1,
               "Invalid unbalance PSI ratio threshold");
  if (small_length > 0 &&
      big_length / small_length >= FLAGS_unbalance_psi_ratio_threshold &&
      big_length > FLAGS_unbalance_psi_larger_party_rows_count_threshold) {
    psi_plan.unbalanced = true;
    // the side with bigger tensor length should be oprf server
    psi_plan.is_server = small_length != self_length;
  }

  std::string server_info;
  if (psi_plan.unbalanced) {
    if (psi_plan.is_server) {
      server_info = ", is server";
    } else {
      server_info = ", is client";
    }
  }
  SPDLOG_DEBUG("CoordinatePsiPlan finished, use UbPsi: {} {}", psi_plan.ublance,
               server_info);

  psi_plan.psi_size_info.self_size = self_length;
  psi_plan.psi_size_info.peer_size = peer_length;

  return psi_plan;
}

namespace {
static constexpr char kPsiInLeft[] = "Left";
static constexpr char kPsiInRight[] = "Right";
static constexpr char kPsiInputPartyCodesAttr[] = "input_party_codes";
}  // namespace

PsiPlan CoordinatePsiPlan(ExecContext* ctx) {
  // get related party codes and ranks
  const auto& my_party_code = ctx->GetSession()->SelfPartyCode();
  std::vector<std::string> input_party_codes =
      ctx->GetStringValuesFromAttribute(kPsiInputPartyCodesAttr);
  bool is_left = my_party_code == input_party_codes.at(0);
  const auto& peer_party_code = input_party_codes.at(is_left ? 1 : 0);
  auto my_rank = ctx->GetSession()->GetPartyRank(my_party_code);
  YACL_ENFORCE(my_rank != -1, "unknown rank for party={}", my_party_code);
  auto peer_rank = ctx->GetSession()->GetPartyRank(peer_party_code);
  YACL_ENFORCE(peer_rank != -1, "unknown rank for party={}", peer_party_code);

  // get input length
  const auto* input_name = is_left ? kPsiInLeft : kPsiInRight;
  const auto& input_pbs = ctx->GetInput(input_name);
  auto* table = ctx->GetTensorTable();
  auto t = table->GetTensor(input_pbs[0].name());
  YACL_ENFORCE(t != nullptr, "tensor {} not found in tensor table",
               input_pbs[0].name());
  int64_t tensor_length = t->Length();
  if (ctx->GetOpType() == "Join") {
    for (int pb_idx = 1; pb_idx < input_pbs.size(); ++pb_idx) {
      auto t = table->GetTensor(input_pbs[pb_idx].name());
      YACL_ENFORCE(t != nullptr, "tensor {} not found in tensor table",
                   input_pbs[pb_idx].name());
      YACL_ENFORCE(tensor_length == t->Length(),
                   "Tensor length in input_pbs should be the same");
    }
  }

  // communicate tensor length
  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, tensor_length);
  auto psi_link = ctx->GetSession()->GetLink();
  auto tag = ctx->GetNodeName() + "-TensorLength";
  if (psi_link->WorldSize() > 2) {
    psi_link = psi_link->SubWorld(tag, input_party_codes);
  }
  auto length_bufs = yacl::link::AllGather(
      psi_link, yacl::ByteContainerView(sbuf.data(), sbuf.size()), tag);
  auto& peer_length_buf = length_bufs[psi_link->NextRank()];
  msgpack::object_handle oh = msgpack::unpack(
      static_cast<char*>(peer_length_buf.data()), peer_length_buf.size());
  auto peer_length = oh.get().as<int64_t>();

  return util::GetPsiPlan(tensor_length, peer_length);
}

BatchProvider::BatchProvider(std::vector<TensorPtr> tensors, bool shuffle,
                             size_t batch_size)
    : tensors_(std::move(tensors)),
      idx_(0),
      shuffle_(shuffle),
      batch_size_(batch_size) {
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

std::vector<std::string> BatchProvider::ReadNextBatch() {
  if (tensors_.empty()) {
    return std::vector<std::string>{};
  }

  auto keys = stringify_visitors_[0]->StringifyBatch(batch_size_);
  if (keys.empty()) {
    return keys;
  }

  for (size_t i = 1; i < tensors_.size(); ++i) {
    auto another_keys = stringify_visitors_[i]->StringifyBatch(batch_size_);

    YACL_ENFORCE(keys.size() == another_keys.size(),
                 "tensor #{} batch size not equals with previous", i);

    keys = Combine(keys, another_keys);
  }

  return keys;
}

std::tuple<std::vector<std::string>, std::vector<size_t>, std::vector<size_t>>
BatchProvider::ReadNextShuffledBatch() {
  std::vector<size_t> batch_indices;
  std::vector<size_t> shuffle_indices;
  if (tensors_.empty()) {
    std::vector<std::string> keys;
    return std::make_tuple(keys, batch_indices, shuffle_indices);
  }

  auto keys = ReadNextBatch();
  batch_indices.resize(keys.size());
  std::iota(batch_indices.begin(), batch_indices.end(), idx_);
  if (shuffle_) {
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
  } else {
    shuffle_indices = batch_indices;
  }
  idx_ += keys.size();

  return std::make_tuple(keys, batch_indices, shuffle_indices);
}

std::vector<std::string> Combine(const std::vector<std::string>& col1,
                                 const std::vector<std::string>& col2) {
  std::vector<std::string> result(col1.size());
  for (size_t i = 0; i < col1.size(); ++i) {
    result[i] = fmt::format("{},{}", col1[i], col2[i]);
  }
  return result;
}

TensorPtr FinalizeAndComputeJoinIndices(
    bool is_left,
    const std::shared_ptr<psi::psi::HashBucketEcPointStore>& self_cache,
    const std::shared_ptr<psi::psi::HashBucketEcPointStore>& peer_cache,
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
    const std::shared_ptr<psi::psi::HashBucketEcPointStore>& left,
    const std::shared_ptr<psi::psi::HashBucketEcPointStore>& right,
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
    const std::shared_ptr<psi::psi::HashBucketEcPointStore>& self_cache,
    const std::shared_ptr<psi::psi::HashBucketEcPointStore>& peer_cache) {
  self_cache->Flush();
  peer_cache->Flush();

  if (is_left) {
    return ComputeInResult(self_cache, peer_cache);
  } else {
    return ComputeInResult(peer_cache, self_cache);
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

TensorPtr ComputeInResult(
    const std::shared_ptr<psi::psi::HashBucketEcPointStore>& left,
    const std::shared_ptr<psi::psi::HashBucketEcPointStore>& right) {
  YACL_ENFORCE(left->num_bins() == right->num_bins(),
               "left store num_bins={} not equal to right store num_bins={}",
               left->num_bins(), right->num_bins());
  InResultResolver resolver;
  for (size_t bin_idx = 0; bin_idx < left->num_bins(); ++bin_idx) {
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

UbPsiCipherStore::UbPsiCipherStore(std::string csv_path, bool enable_cache)
    : csv_path_(std::move(csv_path)), enable_cache_(enable_cache) {
  out_ =
      psi::psi::io::BuildOutputStream(psi::psi::io::FileIoOptions(csv_path_));
  YACL_ENFORCE(out_, "Fail to build outputstream for UbPsiCipherStore");
  out_->Write(kDummyField);
  out_->Write("\n");
}

UbPsiCipherStore::~UbPsiCipherStore() { out_->Close(); }

void UbPsiCipherStore::Save(std::string ciphertext) {
  out_->Write(fmt::format("{}\n", ciphertext));
  if (enable_cache_) {
    data_indices_[ciphertext].push_back(item_count_);
  }
  ++item_count_;
  if (item_count_ % kLogInterval == 0) {
    SPDLOG_DEBUG("client_items_count_={}", item_count_);
  }
}

std::vector<std::string> FinalizeAndComputeIntersection(
    const std::shared_ptr<UbPsiCipherStore>& client_store,
    const std::shared_ptr<UbPsiCipherStore>& server_store) {
  SPDLOG_DEBUG("Begin ComputeIntersection");

  client_store->Finalize();
  server_store->Finalize();

  std::vector<std::string> fields{UbPsiCipherStore::kDummyField};
  psi::psi::CsvBatchProvider server_provider(server_store->GetPath(), fields,
                                             util::kBatchSize);
  size_t batch_count = 0;

  // may have duplicate items
  std::vector<std::string> results;

  while (true) {
    auto batch_server_data = server_provider.ReadNextBatch();
    SPDLOG_DEBUG("read compare batch {}", batch_count);

    if (batch_server_data.empty()) {
      break;
    }

    for (const std::string& server_item : batch_server_data) {
      auto search_ret = client_store->SearchIndices(server_item);
      if (search_ret.has_value()) {
        results.push_back(server_item);
      }
    }

    ++batch_count;
  }

  SPDLOG_DEBUG("ComputeIntersection, batch_count:{}", batch_count);
  SPDLOG_DEBUG("End ComputeIntersection");
  return results;
}

TensorPtr FinalizeAndComputeOprfInResult(
    const std::shared_ptr<UbPsiCipherStore>& client_store,
    const std::shared_ptr<UbPsiCipherStore>& server_store) {
  SPDLOG_DEBUG("Begin ComputeInResult");

  client_store->Finalize();
  server_store->Finalize();

  std::unordered_set<uint64_t> client_indices;
  BooleanTensorBuilder result_builder;

  std::vector<std::string> fields{UbPsiCipherStore::kDummyField};
  psi::psi::CsvBatchProvider server_provider(server_store->GetPath(), fields,
                                             util::kBatchSize);
  size_t batch_count = 0;

  while (true) {
    auto batch_server_data = server_provider.ReadNextBatch();
    SPDLOG_DEBUG("read compare batch {}", batch_count);

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

    ++batch_count;
  }

  SPDLOG_DEBUG("ComputeInResult, batch_count:{}", batch_count);
  SPDLOG_DEBUG("ComputeInResult finish matching");

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
  SPDLOG_DEBUG("Begin ComputeJoinResult");
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
  psi::psi::CsvBatchProvider server_provider(server_store->GetPath(), fields,
                                             util::kBatchSize);
  size_t batch_count = 0;
  while (true) {
    auto batch_server_data = server_provider.ReadNextBatch();
    SPDLOG_DEBUG("read compare batch {}", batch_count);

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

    ++batch_count;
    SPDLOG_DEBUG("ComputeInResult, batch_count:{}", batch_count);
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
  SPDLOG_DEBUG("ComputeJoinResult finish matching");

  TensorPtr result_tensor;
  result_builder.Finish(&result_tensor);
  return {result_tensor, matched_seqs};
}

void UbPsiJoinCache::SaveData(yacl::ByteContainerView item, size_t index,
                              size_t shuffle_index) {
  seq_to_indice_[index] = shuffle_index;
}

// OPRF ECDH PSI phases
void OprfPsiServerTransferServerItems(
    ExecContext* ctx, std::shared_ptr<yacl::link::Context> psi_link,
    const std::shared_ptr<util::BatchProvider>& batch_provider,
    const std::shared_ptr<psi::psi::EcdhOprfPsiServer>& ec_oprf_psi_server,
    std::shared_ptr<psi::psi::IUbPsiCache> ub_cache) {
  SPDLOG_DEBUG(
      "Oprf server start to transfer evaluated server items, my rank: {}, my "
      "party_code: {}",
      ctx->GetSession()->SelfRank(), ctx->GetSession()->SelfPartyCode());

  yacl::link::Barrier(psi_link, "Sync for UbPsi client and server");

  size_t self_item_count =
      ub_cache
          ? ec_oprf_psi_server->FullEvaluateAndSend(batch_provider, ub_cache)
          : ec_oprf_psi_server->FullEvaluateAndSend(batch_provider);

  SPDLOG_INFO("Oprf server: evaluate and send {} items", self_item_count);
}

void OprfPsiServerTransferClientItems(
    ExecContext* ctx,
    const std::shared_ptr<psi::psi::EcdhOprfPsiServer>& ec_oprf_psi_server) {
  SPDLOG_DEBUG(
      "Oprf server start to transfer client items, my rank: {}, my "
      "party_code: {}",
      ctx->GetSession()->SelfRank(), ctx->GetSession()->SelfPartyCode());

  ec_oprf_psi_server->RecvBlindAndSendEvaluate();
  SPDLOG_DEBUG("Oprf server finish transferring client items");
}

void OprfPsiClientTransferServerItems(
    ExecContext* ctx, std::shared_ptr<yacl::link::Context> psi_link,
    const psi::psi::EcdhOprfPsiOptions& psi_options,
    const std::shared_ptr<UbPsiCipherStore>& cipher_store) {
  SPDLOG_DEBUG(
      "Oprf client start to receive evaluated server items, my rank: {}, my "
      "party_code: {}",
      ctx->GetSession()->SelfRank(), ctx->GetSession()->SelfPartyCode());
  auto ec_oprf_psi_client_offline =
      std::make_shared<psi::psi::EcdhOprfPsiClient>(psi_options);

  yacl::link::Barrier(psi_link, "Sync for UbPsi client and server");

  ec_oprf_psi_client_offline->RecvFinalEvaluatedItems(cipher_store);
  SPDLOG_DEBUG(
      "Oprf client finish receiving evaluated server items, items count: {}",
      cipher_store->ItemCount());
}

void OprfPsiClientTransferClientItems(
    ExecContext* ctx,
    const std::shared_ptr<util::BatchProvider>& batch_provider,
    const psi::psi::EcdhOprfPsiOptions& psi_options,
    const std::shared_ptr<UbPsiCipherStore>& cipher_store) {
  SPDLOG_DEBUG(
      "Oprf client start to transfer client items, my rank: {}, my party_code: "
      "{}",
      ctx->GetSession()->SelfRank(), ctx->GetSession()->SelfPartyCode());

  auto ec_oprf_psi_client_online =
      std::make_shared<psi::psi::EcdhOprfPsiClient>(psi_options);

  std::future<size_t> f_client_send_blind = std::async([&] {
    return ec_oprf_psi_client_online->SendBlindedItems(batch_provider);
  });
  ec_oprf_psi_client_online->RecvEvaluatedItems(cipher_store);
  size_t self_items_count = f_client_send_blind.get();
  SPDLOG_INFO("Oprf client send {} blinded items in UbPsiClientOnline",
              self_items_count);

  SPDLOG_DEBUG("Oprf client finish transferring client items, client_count: {}",
               cipher_store->ItemCount());
}

void OprfServerTransferShuffledClientItems(
    ExecContext* ctx,
    const std::shared_ptr<psi::psi::EcdhOprfPsiServer>& ec_oprf_psi_server,
    const std::string& server_cache_path, size_t batch_size,
    std::vector<uint64_t>* matched_indices, size_t* self_item_count) {
  SPDLOG_DEBUG(
      "Oprf server start to transfer shuffled client items, my rank: {}, my "
      "party_code: {}",
      ctx->GetSession()->SelfRank(), ctx->GetSession()->SelfPartyCode());
  ec_oprf_psi_server->RecvBlindAndShuffleSendEvaluate();

  std::shared_ptr<psi::psi::IShuffledBatchProvider> cache_provider =
      std::make_shared<psi::psi::UbPsiCacheProvider>(
          server_cache_path, kNumBins, ec_oprf_psi_server->GetCompareLength());

  std::tie(*matched_indices, *self_item_count) =
      ec_oprf_psi_server->RecvIntersectionMaskedItems(cache_provider);
}

void OprfCLientTransferShuffledClientItems(
    ExecContext* ctx,
    const std::shared_ptr<util::BatchProvider>& batch_provider,
    const psi::psi::EcdhOprfPsiOptions& psi_options,
    const std::shared_ptr<UbPsiCipherStore>& client_store,
    const std::shared_ptr<UbPsiCipherStore>& server_store) {
  SPDLOG_DEBUG(
      "Oprf client start to transfer shuffled client items, my rank: {}, my "
      "party_code: {}",
      ctx->GetSession()->SelfRank(), ctx->GetSession()->SelfPartyCode());

  std::vector<uint8_t> private_key =
      yacl::crypto::RandBytes(psi::psi::kEccKeySize);
  auto ub_psi_client_shuffle_online =
      std::make_shared<psi::psi::EcdhOprfPsiClient>(psi_options, private_key);

  size_t self_items_count =
      ub_psi_client_shuffle_online->SendBlindedItems(batch_provider);
  SPDLOG_INFO("Oprf client send {} blinded items in UbPsiClientShuffleOnline",
              self_items_count);

  ub_psi_client_shuffle_online->RecvEvaluatedItems(client_store);

  auto matched_items =
      util::FinalizeAndComputeIntersection(client_store, server_store);
  std::shared_ptr<psi::psi::IBasicBatchProvider> intersection_masked_provider =
      std::make_shared<psi::psi::MemoryBatchProvider>(matched_items,
                                                      kBatchSize);
  ub_psi_client_shuffle_online->SendIntersectionMaskedItems(
      intersection_masked_provider);
}

}  // namespace scql::engine::util
