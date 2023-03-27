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

#include <shared_mutex>

#include "yacl/link/transport/channel.h"

namespace scql::engine {

// Listener contains the Channels belong to the same Context.
class Listener {
 public:
  Listener() = default;

  ~Listener() = default;

  void AddChannel(const size_t rank,
                  std::shared_ptr<yacl::link::IChannel> channel);

  void OnMessage(const size_t rank, const std::string& key,
                 const std::string& value);

  void OnChunkedMessage(const size_t rank, const std::string& key,
                        const std::string& value, const size_t offset,
                        const size_t total_length);

 private:
  std::map<size_t, std::shared_ptr<yacl::link::IChannel>> channels_;
};

// thread safe, and will be used cocurrently.
class ListenerManager {
 public:
  ListenerManager() = default;

  ~ListenerManager() = default;

  void AddListener(const std::string& link_id,
                   std::shared_ptr<Listener> listener);

  void RemoveListener(const std::string& link_id);

  std::shared_ptr<Listener> GetListener(const std::string& link_id);

 private:
  std::shared_mutex mutex_;
  std::map<std::string, std::shared_ptr<Listener>> listeners_;
};

}  // namespace scql::engine