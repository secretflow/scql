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

#include "engine/link/mux_link_factory.h"

#include "brpc/closure_guard.h"
#include "bthread/bthread.h"
#include "bthread/condition_variable.h"
#include "spdlog/spdlog.h"

namespace scql::engine {

// NOTE: Throw NetworkError for ErrorCode::LINKID_NOT_FOUND:
// since peer's Context corresponding to link_id_ may be available later,
// while ConnectToMesh() in lib-yacl only catch NetworkError and
// retry.
#ifndef THROW_IF_RPC_NOT_OK
#define THROW_IF_RPC_NOT_OK(cntl, response, request_info)                     \
  do {                                                                        \
    if ((cntl).Failed()) {                                                    \
      YACL_THROW_NETWORK_ERROR("send failed: {}, rpc failed={}, message={}.", \
                               request_info, (cntl).ErrorCode(),              \
                               (cntl).ErrorText());                           \
    } else if ((response).error_code() != link::pb::ErrorCode::SUCCESS) {     \
      std::string error_info = fmt::format(                                   \
          "send failed: {}, peer failed code={}, message={}.", request_info,  \
          static_cast<int>((response).error_code()), (response).error_msg()); \
      if ((response).error_code() == link::pb::ErrorCode::LINKID_NOT_FOUND) { \
        YACL_THROW_NETWORK_ERROR(error_info);                                 \
      }                                                                       \
      YACL_THROW(error_info);                                                 \
    }                                                                         \
  } while (false)
#endif

std::shared_ptr<yacl::link::Context> MuxLinkFactory::CreateContext(
    const yacl::link::ContextDesc& desc, size_t self_rank) {
  const size_t world_size = desc.parties.size();
  YACL_ENFORCE(self_rank < world_size,
               "invalid arg: self rank={} not small than world_size={}",
               self_rank, world_size);
  // 1. create channels.
  std::vector<std::shared_ptr<yacl::link::transport::IChannel>> channels(
      world_size);
  auto listener = std::make_shared<Listener>();
  for (size_t rank = 0; rank < world_size; rank++) {
    if (rank == self_rank) {
      continue;
    }
    const auto& peer_host = desc.parties[rank].host;
    auto rpc_channel =
        channel_manager_->Create(peer_host, RemoteRole::PeerEngine);
    YACL_ENFORCE(rpc_channel, "create rpc channel failed for rank={}", rank);
    auto link_channel = std::make_shared<MuxLinkChannel>(
        self_rank, rank, desc.recv_timeout_ms, desc.http_max_payload_size,
        desc.id, rpc_channel);
    channels[rank] = link_channel;
    if (rank == self_rank) {
      continue;
    }
    listener->AddChannel(rank, link_channel);
  }
  listener_manager_->AddListener(desc.id, listener);
  // 3. construct Context.
  auto ctx = std::make_shared<yacl::link::Context>(
      desc, self_rank, std::move(channels), nullptr, false);
  return ctx;
}

void MuxLinkChannel::SendImpl(const std::string& key,
                              yacl::ByteContainerView value) {
  SendImpl(key, value, 0);
}

void MuxLinkChannel::SendImpl(const std::string& key,
                              yacl::ByteContainerView value,
                              uint32_t timeout_ms) {
  if (value.size() > http_max_payload_size_) {
    SendChunked(key, value);
    return;
  }

  link::pb::MuxPushRequest request;
  {
    request.set_link_id(link_id_);
    auto* msg = request.mutable_msg();
    msg->set_sender_rank(self_rank_);
    msg->set_key(key);
    msg->set_value(value.data(), value.size());
    msg->set_trans_type(link::pb::TransType::MONO);
  }

  link::pb::MuxPushResponse response;
  brpc::Controller cntl;
  if (timeout_ms != 0) {
    cntl.set_timeout_ms(timeout_ms);
  }
  link::pb::MuxReceiverService::Stub stub(rpc_channel_.get());
  stub.Push(&cntl, &request, &response, nullptr);

  std::string request_info = fmt::format(
      "link_id={} sender_rank={} send key={}", link_id_, self_rank_, key);
  THROW_IF_RPC_NOT_OK(cntl, response, request_info);
}

namespace {

class BatchDesc {
 protected:
  size_t batch_idx_;
  size_t batch_size_;
  size_t total_size_;

 public:
  BatchDesc(size_t batch_idx, size_t batch_size, size_t total_size)
      : batch_idx_(batch_idx),
        batch_size_(batch_size),
        total_size_(total_size) {}

  // return the index of this batch.
  size_t Index() const { return batch_idx_; }

  // return the offset of the first element in this batch.
  size_t Begin() const { return batch_idx_ * batch_size_; }

  // return the offset after last element in this batch.
  size_t End() const { return std::min(Begin() + batch_size_, total_size_); }

  // return the size of this batch.
  size_t Size() const { return End() - Begin(); }

  std::string ToString() const { return "B:" + std::to_string(batch_idx_); };
};

}  // namespace

void MuxLinkChannel::SendChunked(const std::string& key,
                                 yacl::ByteContainerView value) {
  const size_t bytes_per_chunk = http_max_payload_size_;
  const size_t num_bytes = value.size();
  const size_t num_chunks = (num_bytes + bytes_per_chunk - 1) / bytes_per_chunk;

  constexpr uint32_t kParallelSize = 10;
  const size_t batch_size = kParallelSize;
  const size_t num_batches = (num_chunks + batch_size - 1) / batch_size;

  for (size_t batch_idx = 0; batch_idx < num_batches; batch_idx++) {
    const BatchDesc batch(batch_idx, batch_size, num_chunks);

    // See: "半同步“ from
    // https://github.com/apache/incubator-brpc/blob/master/docs/cn/client.md
    std::vector<brpc::Controller> cntls(batch.Size());
    std::vector<link::pb::MuxPushResponse> responses(batch.Size());

    // fire batched chunk requests.
    for (size_t idx = 0; idx < batch.Size(); idx++) {
      const size_t chunk_idx = batch.Begin() + idx;
      const size_t chunk_offset = chunk_idx * bytes_per_chunk;

      link::pb::MuxPushRequest request;
      {
        request.set_link_id(link_id_);
        auto* msg = request.mutable_msg();
        msg->set_sender_rank(self_rank_);
        msg->set_key(key);
        msg->set_value(value.data() + chunk_offset,
                       std::min(bytes_per_chunk, value.size() - chunk_offset));
        msg->set_trans_type(link::pb::TransType::CHUNKED);
        msg->mutable_chunk_info()->set_chunk_offset(chunk_offset);
        msg->mutable_chunk_info()->set_message_length(num_bytes);
      }
      auto& cntl = cntls[idx];
      auto& response = responses[idx];
      link::pb::MuxReceiverService::Stub stub(rpc_channel_.get());
      stub.Push(&cntl, &request, &response, brpc::DoNothing());
    }

    for (size_t idx = 0; idx < batch.Size(); idx++) {
      brpc::Join(cntls[idx].call_id());
    }

    for (size_t idx = 0; idx < batch.Size(); idx++) {
      const size_t chunk_idx = batch.Begin() + idx;
      std::string request_info = fmt::format(
          "link_id={}, sender_rank={}, key={} (chunked {} out of {})", link_id_,
          self_rank_, key, chunk_idx + 1, num_chunks);
      THROW_IF_RPC_NOT_OK(cntls[idx], responses[idx], request_info);
    }
  }
}

}  // namespace scql::engine
