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

class MuxLinkChannel : public yacl::link::transport::ChannelBase {
 public:
  MuxLinkChannel(size_t self_rank, size_t peer_rank,
                 size_t http_max_payload_size, std::string link_id,
                 std::shared_ptr<::google::protobuf::RpcChannel> channel)
      : ChannelBase(self_rank, peer_rank, false),
        http_max_payload_size_(http_max_payload_size),
        link_id_(std::move(link_id)),
        rpc_channel_(std::move(channel)) {}

  MuxLinkChannel(size_t self_rank, size_t peer_rank, size_t recv_timeout_ms,
                 size_t http_max_payload_size, std::string link_id,
                 std::shared_ptr<::google::protobuf::RpcChannel> channel)
      : ChannelBase(self_rank, peer_rank, recv_timeout_ms, false),
        http_max_payload_size_(http_max_payload_size),
        link_id_(std::move(link_id)),
        rpc_channel_(std::move(channel)) {}

 private:
  void SendChunked(const std::string& key, yacl::ByteContainerView value);

  void SendImpl(const std::string& key, yacl::ByteContainerView value) override;

  void SendImpl(const std::string& key, yacl::ByteContainerView value,
                uint32_t timeout_ms) override;

 private:
  size_t http_max_payload_size_;
  std::string link_id_;
  const std::shared_ptr<::google::protobuf::RpcChannel> rpc_channel_;
};

}  // namespace scql::engine