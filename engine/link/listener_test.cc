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

#include "gtest/gtest.h"
#include "yacl/link/transport/channel_mem.h"

namespace scql::engine {

TEST(ListenerTest, Works) {
  // Given
  Listener listener;
  size_t self_rank = 0;
  size_t peer_rank = 1;
  size_t not_exist_rank = 100;
  const std::string kAckKey = {'A', 'C', 'K', '\x01', '\x00'};
  // When
  EXPECT_NO_THROW(listener.AddChannel(
      peer_rank,
      std::make_shared<yacl::link::ChannelMem>(self_rank, peer_rank)));
  // Then
  EXPECT_THROW(
      listener.AddChannel(peer_rank, std::make_shared<yacl::link::ChannelMem>(
                                         self_rank, peer_rank)),
      ::yacl::EnforceNotMet);

  EXPECT_THROW(listener.OnMessage(not_exist_rank, kAckKey, "value"),
               ::yacl::EnforceNotMet);
  EXPECT_NO_THROW(listener.OnMessage(peer_rank, kAckKey, "value"));

  EXPECT_THROW(listener.OnChunkedMessage(not_exist_rank, "key", "value", 0, 10),
               ::yacl::EnforceNotMet);
  EXPECT_NO_THROW(listener.OnChunkedMessage(peer_rank, "key", "value", 0, 10));
}

TEST(ListenerManagerTest, works) {
  // Given
  auto listener = std::make_shared<Listener>();
  ListenerManager mgr;
  std::string link_id = "test-link-id";
  std::string not_exist_link_id = "not-exist-link-id";

  // When
  EXPECT_NO_THROW(mgr.AddListener(link_id, listener));
  // Then
  EXPECT_THROW(mgr.AddListener(link_id, listener), ::yacl::LogicError);
  EXPECT_EQ(nullptr, mgr.GetListener(not_exist_link_id));
  EXPECT_NE(nullptr, mgr.GetListener(link_id));

  // When
  mgr.RemoveListener(link_id);
  // Then
  EXPECT_EQ(nullptr, mgr.GetListener(link_id));
}

}  // namespace scql::engine