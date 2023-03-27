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

#include "engine/link/channel_manager.h"

#include "spdlog/spdlog.h"
#include "yacl/base/exception.h"

namespace scql::engine {

void ChannelManager::AddChannelOptions(const RemoteRole role,
                                       const brpc::ChannelOptions& options) {
  auto iter = options_.find(role);
  if (iter != options_.end()) {
    YACL_THROW_LOGIC_ERROR("options already exist for role={}",
                           static_cast<int>(role));
  }
  options_.emplace(role, options);
}

std::shared_ptr<google::protobuf::RpcChannel> ChannelManager::Create(
    const std::string& remote_addr, RemoteRole role) {
  auto remote_pair = std::pair(role, remote_addr);
  {
    absl::ReaderMutexLock lock(&mu_);

    auto iter = channels_.find(remote_pair);
    if (iter != channels_.end()) {
      return iter->second;
    }
  }

  brpc::ChannelOptions option;
  auto iter = options_.find(role);
  if (iter != options_.end()) {
    option = iter->second;
  } else {
    SPDLOG_WARN("not found options for role={}, default use http protocal",
                static_cast<int>(role));
    option.protocol = "http:proto";
  }
  auto result = std::make_shared<brpc::Channel>();
  int init_result = result->Init(remote_addr.c_str(), &option);
  if (init_result != 0) {
    YACL_THROW(
        "BrpcChannel Init failed, ret={}, remote_addr={}, role={}, protocal={}",
        init_result, remote_addr, static_cast<int>(role), option.protocol);
  }

  {
    absl::MutexLock lock(&mu_);
    channels_[remote_pair] = result;
  }

  return result;
}

}  // namespace scql::engine
