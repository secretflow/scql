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
      if ((cntl).ErrorCode() == brpc::EHTTP) {                                \
        YACL_THROW_LINK_ERROR(                                                \
            0, (cntl).http_response().status_code(),                          \
            "send failed: {}, http code={}, message={}.", request_info,       \
            static_cast<int>((cntl).http_response().status_code()),           \
            (cntl).ErrorText());                                              \
      }                                                                       \
      YACL_THROW_LINK_ERROR((cntl).ErrorCode(), 0,                            \
                            "send failed: {}, rpc failed={}, message={}.",    \
                            request_info, (cntl).ErrorCode(),                 \
                            (cntl).ErrorText());                              \
    } else if ((response).error_code() != link::pb::ErrorCode::SUCCESS) {     \
      std::string error_info = fmt::format(                                   \
          "send failed: {}, peer failed code={}, message={}.", request_info,  \
          static_cast<int>((response).error_code()), (response).error_msg()); \
      if ((response).error_code() == link::pb::ErrorCode::LINKID_NOT_FOUND) { \
        YACL_THROW_NETWORK_ERROR("{}", error_info);                           \
      }                                                                       \
      YACL_THROW("{}", error_info);                                           \
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
    auto rpc_channel = channel_manager_->Create(
        spdlog::default_logger(), peer_host, RemoteRole::PeerEngine);
    YACL_ENFORCE(rpc_channel, "create rpc channel failed for rank={}", rank);
    auto link = std::make_shared<MuxLink>(
        self_rank, rank, desc.http_max_payload_size, desc.id, rpc_channel);

    auto link_channel = std::make_shared<yacl::link::transport::Channel>(
        link, desc.recv_timeout_ms, false, desc.retry_opts);
    channels[rank] = link_channel;
    listener->AddChannel(rank, link_channel);
  }
  listener_manager_->AddListener(desc.id, listener);
  // 3. construct Context.
  auto ctx = std::make_shared<yacl::link::Context>(
      desc, self_rank, std::move(channels), nullptr, false);
  return ctx;
}

size_t MuxLink::GetMaxBytesPerChunk() const { return http_max_payload_size_; }

void MuxLink::SetMaxBytesPerChunk(size_t bytes) {
  http_max_payload_size_ = bytes;
}

std::unique_ptr<::google::protobuf::Message> MuxLink::PackMonoRequest(
    const std::string& key, yacl::ByteContainerView value) const {
  auto request = std::make_unique<link::pb::MuxPushRequest>();
  {
    request->set_link_id(link_id_);
    auto* msg = request->mutable_msg();
    msg->set_sender_rank(self_rank_);
    msg->set_key(key);
    msg->set_value(value.data(), value.size());
    msg->set_trans_type(link::pb::TransType::MONO);
  }
  return request;
}

std::unique_ptr<::google::protobuf::Message> MuxLink::PackChunkedRequest(
    const std::string& key, yacl::ByteContainerView value, size_t offset,
    size_t total_length) const {
  auto request = std::make_unique<link::pb::MuxPushRequest>();
  {
    request->set_link_id(link_id_);
    auto* msg = request->mutable_msg();
    msg->set_sender_rank(self_rank_);
    msg->set_key(key);
    msg->set_value(value.data(), value.size());
    msg->set_trans_type(link::pb::TransType::CHUNKED);
    msg->mutable_chunk_info()->set_chunk_offset(offset);
    msg->mutable_chunk_info()->set_message_length(total_length);
  }
  return request;
}

void MuxLink::UnpackMonoRequest(const ::google::protobuf::Message& request,
                                std::string* key,
                                yacl::ByteContainerView* value) const {
  auto real_request = static_cast<const link::pb::MuxPushRequest*>(&request);
  *key = real_request->msg().key();
  *value = real_request->msg().value();
}

void MuxLink::UnpackChunckRequest(const ::google::protobuf::Message& request,
                                  std::string* key,
                                  yacl::ByteContainerView* value,
                                  size_t* offset, size_t* total_length) const {
  auto real_request = static_cast<const link::pb::MuxPushRequest*>(&request);
  *key = real_request->msg().key();
  *value = real_request->msg().value();
  *offset = real_request->msg().chunk_info().chunk_offset();
  *total_length = real_request->msg().chunk_info().message_length();
}

void MuxLink::FillResponseOk(const ::google::protobuf::Message& request,
                             ::google::protobuf::Message* response) const {
  auto real_response = static_cast<link::pb::MuxPushResponse*>(response);
  real_response->set_error_code(link::pb::ErrorCode::SUCCESS);
  real_response->set_error_msg("");
}

void MuxLink::FillResponseError(const ::google::protobuf::Message& request,
                                ::google::protobuf::Message* response) const {
  auto real_response = static_cast<link::pb::MuxPushResponse*>(response);
  auto real_request = static_cast<const link::pb::MuxPushRequest*>(&request);

  real_response->set_error_code(link::pb::ErrorCode::INVALID_REQUEST);
  real_response->set_error_msg(
      fmt::format("Error: trans type={}, from link_id={} rank={}",
                  TransType_Name(real_request->msg().trans_type()), link_id_,
                  real_request->msg().sender_rank()));
}

bool MuxLink::IsChunkedRequest(
    const ::google::protobuf::Message& request) const {
  auto real_request = static_cast<const link::pb::MuxPushRequest*>(&request);
  return real_request->msg().trans_type() == link::pb::TransType::CHUNKED;
}

bool MuxLink::IsMonoRequest(const ::google::protobuf::Message& request) const {
  auto real_request = static_cast<const link::pb::MuxPushRequest*>(&request);
  return real_request->msg().trans_type() == link::pb::TransType::MONO;
}

void MuxLink::SendRequest(const ::google::protobuf::Message& request,
                          uint32_t timeout) const {
  link::pb::MuxPushResponse response;
  brpc::Controller cntl;
  if (timeout != 0) {
    cntl.set_timeout_ms(timeout);
  }
  link::pb::MuxReceiverService::Stub stub(rpc_channel_.get());
  auto push_request = static_cast<const link::pb::MuxPushRequest*>(&request);
  stub.Push(&cntl, push_request, &response, nullptr);

  std::string request_info =
      fmt::format("link_id={} sender_rank={} send key={}", link_id_,
                  LocalRank(), push_request->msg().key());
  THROW_IF_RPC_NOT_OK(cntl, response, request_info);
}

}  // namespace scql::engine
