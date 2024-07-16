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

#include "absl/strings/match.h"
#include "spdlog/spdlog.h"
#include "yacl/base/exception.h"

namespace scql::engine {

void ChannelManager::AddChannelOptions(const RemoteRole role,
                                       const ChannelOptions& options) {
  auto iter = options_.find(role);
  if (iter != options_.end()) {
    YACL_THROW_LOGIC_ERROR("options already exist for role={}",
                           static_cast<int>(role));
  }
  options_.emplace(role, options);
}

std::shared_ptr<google::protobuf::RpcChannel> ChannelManager::Create(
    const std::shared_ptr<spdlog::logger>& logger,
    const std::string& remote_addr, RemoteRole role) {
  ChannelOptions options;
  auto iter = options_.find(role);
  if (iter != options_.end()) {
    options = iter->second;
  } else {
    SPDLOG_LOGGER_WARN(
        logger, "not found options for role={}, default use http protocal",
        static_cast<int>(role));
    options.brpc_options.protocol = "http:proto";
  }
  auto result = std::make_shared<brpc::Channel>();
  auto addr = remote_addr;
  // add "http://" prefix if load balancer is not empty and no "http[s]://"
  // prefix
  if (options.load_balancer != "" && !absl::StartsWith(addr, "http://") &&
      !absl::StartsWith(addr, "https://")) {
    addr = absl::StrCat("http://", addr);
  }
  int init_result = result->Init(addr.c_str(), options.load_balancer.c_str(),
                                 &(options.brpc_options));
  if (init_result != 0) {
    YACL_THROW(
        "BrpcChannel Init failed, ret={}, remote_addr={}, load_balancer={}, "
        "role={}, protocol={}",
        init_result, addr, static_cast<int>(role), options.load_balancer,
        options.brpc_options.protocol.name());
  }
  return result;
}

}  // namespace scql::engine
