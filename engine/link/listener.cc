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

#include "engine/link/listener.h"

#include "spdlog/spdlog.h"

namespace scql::engine {

void Listener::AddChannel(
    const size_t rank,
    std::shared_ptr<yacl::link::transport::Channel> channel) {
  YACL_ENFORCE(channel, "add channel failed, channel can't be nullptr.");
  YACL_ENFORCE(channels_.count(rank) == 0,
               "add channel failed, rank={} exists before add.", rank);
  channels_.emplace(rank, channel);
}

void Listener::OnRequest(const size_t rank,
                         const link::pb::MuxPushRequest* request,
                         link::pb::MuxPushResponse* response) {
  auto iter = channels_.find(rank);
  YACL_ENFORCE(iter != channels_.end(), "channel for rank:{} not exist", rank);
  YACL_ENFORCE(iter->second, "channel for rank:{} is nullptr", rank);
  iter->second->OnRequest(*request, response);
}

void ListenerManager::AddListener(const std::string& link_id,
                                  std::shared_ptr<Listener> listener) {
  std::unique_lock lock(mutex_);
  if (listeners_.count(link_id) != 0) {
    YACL_THROW_LOGIC_ERROR("link_id:{} exist before add Listener.", link_id);
  }
  listeners_.emplace(link_id, listener);
}

void ListenerManager::RemoveListener(const std::string& link_id) {
  std::unique_lock lock(mutex_);
  if (listeners_.count(link_id) == 0) {
    SPDLOG_WARN("link_id:{} not exist before remove Listener.", link_id);
    return;
  }
  listeners_.erase(link_id);
}

std::shared_ptr<Listener> ListenerManager::GetListener(
    const std::string& link_id) {
  std::shared_lock lock(mutex_);
  auto iter = listeners_.find(link_id);
  if (iter == listeners_.end()) {
    SPDLOG_WARN("Listener for link_id:{} not exist.", link_id);
    return nullptr;
  }
  return iter->second;
}

}  // namespace scql::engine
