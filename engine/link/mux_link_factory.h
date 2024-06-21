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

#include <algorithm>
#include <atomic>
#include <memory>

#include "brpc/channel.h"
#include "yacl/link/factory.h"

#include "engine/link/channel_manager.h"
#include "engine/link/listener.h"

#include "engine/link/mux_receiver.pb.h"

namespace scql::engine {

class MuxLinkFactory : public yacl::link::ILinkFactory {
 public:
  explicit MuxLinkFactory(ChannelManager* channel_manager,
                          ListenerManager* listener_manager)
      : channel_manager_(channel_manager),
        listener_manager_(listener_manager) {}

  /// @brief add listener to listener manager after link context created.
  std::shared_ptr<yacl::link::Context> CreateContext(
      const yacl::link::ContextDesc& desc, size_t self_rank) override;

 private:
  ChannelManager* channel_manager_;
  ListenerManager* listener_manager_;
};

class MuxLink : public yacl::link::transport::TransportLink {
 public:
  MuxLink(size_t self_rank, size_t peer_rank, size_t http_max_payload_size,
          std::string link_id,
          std::shared_ptr<::google::protobuf::RpcChannel> rpc_channel)
      : TransportLink(self_rank, peer_rank),
        http_max_payload_size_(http_max_payload_size),
        link_id_(std::move(link_id)),
        rpc_channel_(std::move(rpc_channel)) {}

  size_t GetMaxBytesPerChunk() const override;
  void SetMaxBytesPerChunk(size_t bytes) override;
  std::unique_ptr<::google::protobuf::Message> PackMonoRequest(
      const std::string& key, yacl::ByteContainerView value) const override;
  std::unique_ptr<::google::protobuf::Message> PackChunkedRequest(
      const std::string& key, yacl::ByteContainerView value, size_t offset,
      size_t total_length) const override;
  void UnpackMonoRequest(const ::google::protobuf::Message& request,
                         std::string* key,
                         yacl::ByteContainerView* value) const override;
  void UnpackChunckRequest(const ::google::protobuf::Message& request,
                           std::string* key, yacl::ByteContainerView* value,
                           size_t* offset, size_t* total_length) const override;
  void FillResponseOk(const ::google::protobuf::Message& request,
                      ::google::protobuf::Message* response) const override;
  void FillResponseError(const ::google::protobuf::Message& request,
                         ::google::protobuf::Message* response) const override;
  bool IsChunkedRequest(
      const ::google::protobuf::Message& request) const override;
  bool IsMonoRequest(const ::google::protobuf::Message& request) const override;

  void SendRequest(const ::google::protobuf::Message& request,
                   uint32_t timeout_override_ms) const override;

 private:
  size_t http_max_payload_size_;
  std::string link_id_;
  const std::shared_ptr<::google::protobuf::RpcChannel> rpc_channel_;
};

}  // namespace scql::engine