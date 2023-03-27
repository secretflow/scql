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

#include "engine/framework/session.h"

#include "arrow/array.h"
#include "arrow/visit_array_inline.h"
#include "openssl/sha.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/primitive_builder.h"
#include "engine/core/string_tensor_builder.h"

namespace scql::engine {

Session::Session(const SessionOptions& session_opt,
                 const pb::SessionStartParams& params,
                 yacl::link::ILinkFactory* link_factory,
                 std::shared_ptr<spdlog::logger> logger, Router* router,
                 DatasourceAdaptorMgr* ds_mgr)
    : id_(params.session_id()),
      session_opt_(session_opt),
      parties_(params),
      state_(SessionState::IDLE),
      link_factory_(link_factory),
      logger_(std::move(logger)),
      router_(router),
      ds_mgr_(ds_mgr) {
  start_time_ = std::chrono::system_clock::now();
  if (logger_ == nullptr) {
    logger_ = spdlog::default_logger();
  }
  tensor_table_ = std::make_unique<TensorTable>();

  InitLink();
  if (lctx_->WorldSize() >= 2) {
    // spu HalContext valid only when world_size >= 2
    spu_hctx_ =
        std::make_unique<spu::HalContext>(params.spu_runtime_cfg(), lctx_);
  }
}

void Session::InitLink() {
  yacl::link::ContextDesc ctx_desc;
  {
    ctx_desc.id = id_;
    ctx_desc.recv_timeout_ms = session_opt_.link_recv_timeout_ms;
    ctx_desc.parties.reserve(parties_.WorldSize());
    for (const auto& party : parties_.AllParties()) {
      yacl::link::ContextDesc::Party p;
      p.id = party.id;
      p.host = party.host;
      ctx_desc.parties.push_back(std::move(p));
    }
  }
  lctx_ = link_factory_->CreateContext(ctx_desc, parties_.SelfRank());
  lctx_->ConnectToMesh();
}

void Session::MergeDeviceSymbolsFrom(const spu::device::SymbolTable& other) {
  for (const auto& kv : other) {
    YACL_ENFORCE(!device_symbols_.hasVar(kv.first), "symbol {} already exists",
                 kv.first);
    device_symbols_.setVar(kv.first, kv.second);
  }
}

namespace {

// The std::hash for std::string is not crypto-safe. Hence it cannot be used to
// simulate a random oracle. Currently we still want the hash function to
// generate a 64 bits fingerprint. Now we choose SHA256 and treat the first 64
// bits as the hash bits. It is not perfect but could be considered random
// enough given the 64 bits output constraint.
//
// Q: Why not std::hash ?
// A: std::hash has a much higher confliction possibility than taking 64bit from
// SHA256 results.
//
// Q: Why `std::hash` is not crypto-safe?
// A: Most compiler choose to implement std::hash by `fast` hashing algo like
// murmurhash .
// See
// https://stackoverflow.com/questions/19411742/what-is-the-default-hash-function-used-in-c-stdunordered-map

static_assert(sizeof(size_t) == 8);
size_t CryptoHash(const std::string& str) {
  std::array<unsigned char, SHA256_DIGEST_LENGTH> hash;
  SHA256_CTX c;

  SHA256_Init(&c);
  SHA256_Update(&c, str.data(), str.size());
  SHA256_Final(hash.data(), &c);

  size_t ret;
  std::memcpy(&ret, hash.data(), sizeof(ret));
  return ret;
}

class StringToHashConverter {
 public:
  explicit StringToHashConverter(
      absl::flat_hash_map<size_t, std::string>* hash_to_string)
      : hash_to_string_(hash_to_string) {
    YACL_ENFORCE(hash_to_string, "hash_to_string can not be null.");
    builder_ = std::make_unique<UInt64TensorBuilder>();
  }

  void GetHashResult(std::shared_ptr<Tensor>* tensor) {
    builder_->Finish(tensor);
  }

  template <typename T>
  arrow::Status Visit(const T& array) {
    return arrow::Status::NotImplemented(
        fmt::format("type {} is not implemented in StringToHashConverter",
                    array.type()->name()));
  }

  arrow::Status Visit(const arrow::StringArray& array) {
    for (int64_t i = 0; i < array.length(); i++) {
      const std::string& cur_str = array.GetString(i);
      size_t hash_value = CryptoHash(cur_str);

      hash_to_string_->emplace(hash_value, cur_str);
      builder_->Append(hash_value);
    }
    return arrow::Status::OK();
  }

 private:
  absl::flat_hash_map<size_t, std::string>* hash_to_string_;
  std::unique_ptr<UInt64TensorBuilder> builder_;
};

class HashToStringConverter {
 public:
  explicit HashToStringConverter(
      absl::flat_hash_map<size_t, std::string>* hash_to_string)
      : hash_to_string_(hash_to_string) {
    YACL_ENFORCE(hash_to_string, "hash_to_string can not be null.");
    builder_ = std::make_unique<StringTensorBuilder>();
  }

  void GetStringResult(std::shared_ptr<Tensor>* tensor) {
    builder_->Finish(tensor);
  }

  template <typename T>
  arrow::Status Visit(const T& array) {
    return arrow::Status::NotImplemented(
        fmt::format("type {} is not implemented in HashToStringConverter",
                    array.type()->name()));
  }

  arrow::Status Visit(const arrow::NumericArray<arrow::UInt64Type>& array) {
    for (int64_t i = 0; i < array.length(); i++) {
      const auto& hash_value = array.GetView(i);
      auto iter = hash_to_string_->find(hash_value);
      if (iter == hash_to_string_->end()) {
        return arrow::Status::Invalid(fmt::format(
            "get string for hash={} failed, not found", hash_value));
      }

      builder_->Append(iter->second);
    }
    return arrow::Status::OK();
  }

 private:
  absl::flat_hash_map<size_t, std::string>* hash_to_string_;
  std::unique_ptr<StringTensorBuilder> builder_;
};

}  // namespace

TensorPtr Session::StringToHash(const Tensor& string_tensor) {
  StringToHashConverter converter(&hash_to_string_values_);
  const auto& chunked_arr = string_tensor.ToArrowChunkedArray();
  for (int i = 0; i < chunked_arr->num_chunks(); ++i) {
    THROW_IF_ARROW_NOT_OK(
        arrow::VisitArrayInline(*(chunked_arr->chunk(i)), &converter));
  }

  TensorPtr result;
  converter.GetHashResult(&result);
  return result;
}

TensorPtr Session::HashToString(const Tensor& hash_tensor) {
  HashToStringConverter converter(&hash_to_string_values_);
  const auto& chunked_arr = hash_tensor.ToArrowChunkedArray();
  for (int i = 0; i < chunked_arr->num_chunks(); ++i) {
    THROW_IF_ARROW_NOT_OK(
        arrow::VisitArrayInline(*(chunked_arr->chunk(i)), &converter));
  }

  TensorPtr result;
  converter.GetStringResult(&result);
  return result;
}

}  // namespace scql::engine