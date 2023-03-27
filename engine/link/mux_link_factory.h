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

class MuxLinkChannel : public yacl::link::ChannelBase,
                       public std::enable_shared_from_this<MuxLinkChannel> {
 public:
  MuxLinkChannel(size_t self_rank, size_t peer_rank,
                 size_t http_max_payload_size, const std::string& link_id,
                 std::shared_ptr<::google::protobuf::RpcChannel> channel)
      : ChannelBase(self_rank, peer_rank),
        http_max_payload_size_(http_max_payload_size),
        link_id_(link_id),
        rpc_channel_(channel) {}

  MuxLinkChannel(size_t self_rank, size_t peer_rank, size_t recv_timeout_ms,
                 size_t http_max_payload_size, const std::string& link_id,
                 std::shared_ptr<::google::protobuf::RpcChannel> channel)
      : ChannelBase(self_rank, peer_rank, recv_timeout_ms),
        http_max_payload_size_(http_max_payload_size),
        link_id_(link_id),
        rpc_channel_(channel) {}

 public:
  // Note: temporarily keep the same realization with yacl/channel_brpc. need to
  // reduce duplication of code.
  void SendChunked(const std::string& key, yacl::ByteContainerView value);

  void WaitAsyncSendToFinish() override {
    std::unique_lock<std::mutex> lock(wait_async_mutex_);
    wait_async_cv_.wait(lock, [&] { return running_async_count_ == 0; });
  }

  void AddAsyncCount() {
    std::unique_lock<std::mutex> lock(wait_async_mutex_);
    running_async_count_++;
  }

  void SubAsyncCount() {
    std::unique_lock<std::mutex> lock(wait_async_mutex_);
    YACL_ENFORCE(running_async_count_ > 0);
    running_async_count_--;
    if (running_async_count_ == 0) {
      wait_async_cv_.notify_all();
    }
  }

 private:
  // NOTE: If meet send failures, SendAsyncImpl will not throw errors, while
  // SendImpl will throw errors.
  void SendAsyncImpl(const std::string& key,
                     yacl::ByteContainerView value) override {
    SendAsyncInternal(key, value);
  }

  void SendAsyncImpl(const std::string& key, yacl::Buffer&& value) override {
    SendAsyncInternal(key, std::move(value));
  }

  void SendImpl(const std::string& key, yacl::ByteContainerView value) override;

  template <class ValueType>
  void SendAsyncInternal(const std::string& key, ValueType&& value);

 private:
  size_t http_max_payload_size_;
  std::string link_id_;
  const std::shared_ptr<::google::protobuf::RpcChannel> rpc_channel_;
  // for async send impl.
  std::condition_variable wait_async_cv_;
  std::mutex wait_async_mutex_;
  int64_t running_async_count_ = 0;
};

}  // namespace scql::engine