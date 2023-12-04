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

#include <memory>

#include "brpc/channel.h"

namespace scql::engine {

enum class RemoteRole {
  Invalid = 0,
  PeerEngine = 1,
  Driver = 2,  // Driver could be SCDB, SCQLBroker...
};

class ChannelManager {
 public:
  void AddChannelOptions(const RemoteRole role,
                         const brpc::ChannelOptions& options);

  std::shared_ptr<google::protobuf::RpcChannel> Create(
      const std::string& remote_addr, RemoteRole role);

 private:
  std::map<RemoteRole, brpc::ChannelOptions> options_;
};

}  // namespace scql::engine