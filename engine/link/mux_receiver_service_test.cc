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

#include "engine/link/mux_receiver_service.h"

#include <future>

#include "brpc/server.h"
#include "gtest/gtest.h"
#include "yacl/link/link.h"

#include "engine/link/mux_link_factory.h"

namespace scql::engine {

//  test with MuxLinkChannel.
class MuxReceiverServiceImplTest : public ::testing::Test {};

TEST_F(MuxReceiverServiceImplTest, Works) {
  constexpr size_t kWorldSize = 3u;
  std::vector<std::unique_ptr<ListenerManager>> listener_managers;
  std::vector<std::unique_ptr<MuxReceiverServiceImpl>> services;
  std::vector<brpc::Server> servers(kWorldSize);
  for (size_t rank = 0; rank < kWorldSize; rank++) {
    listener_managers.emplace_back(new ListenerManager());

    services.emplace_back(
        new MuxReceiverServiceImpl(listener_managers[rank].get()));
    servers[rank].AddService(services[rank].get(),
                             brpc::SERVER_DOESNT_OWN_SERVICE);

    brpc::ServerOptions options;
    servers[rank].Start("127.0.0.1:0", &options);
  }

  yacl::link::ContextDesc link_desc;
  for (size_t rank = 0; rank < kWorldSize; rank++) {
    const std::string rank_host =
        butil::endpoint2str(servers[rank].listen_address()).c_str();
    link_desc.parties.push_back({fmt::format("party{}", rank), rank_host});
  }
  std::string new_id = link_desc.id + "new";

  auto proc = [&](yacl::link::ILinkFactory* factory, size_t self_rank) -> void {
    std::shared_ptr<yacl::link::Context> lc0, lc1;
    EXPECT_NO_THROW(lc0 = factory->CreateContext(link_desc, self_rank));
    EXPECT_TRUE(lc0 != nullptr);
    EXPECT_NO_THROW(lc0->ConnectToMesh());
    EXPECT_NO_THROW(
        yacl::link::Barrier(lc0, "ba-0"));  // simple function works.

    // create with same link id failed.
    EXPECT_THROW(lc1 = factory->CreateContext(link_desc, self_rank),
                 ::yacl::LogicError);

    // lc0 still works
    EXPECT_NO_THROW(
        yacl::link::Barrier(lc0, "ba-1"));  // simple function works.

    // release lc0
    lc0->WaitLinkTaskFinish();
    lc0.reset();
    // create with a new id.
    link_desc.id = new_id;
    EXPECT_NO_THROW(lc1 = factory->CreateContext(link_desc, self_rank));
    EXPECT_TRUE(lc1 != nullptr);
    EXPECT_NO_THROW(lc1->ConnectToMesh());
    EXPECT_NO_THROW(
        yacl::link::Barrier(lc1, "ba-2"));  // simple function works.
    lc1->WaitLinkTaskFinish();
  };

  std::vector<std::future<void>> futures(kWorldSize);
  ChannelManager channel_manager;
  std::vector<std::unique_ptr<MuxLinkFactory>> mux_link_factorys;
  for (size_t rank = 0; rank < kWorldSize; rank++) {
    mux_link_factorys.emplace_back(
        new MuxLinkFactory(&channel_manager, listener_managers[rank].get()));
    futures[rank] = std::async(proc, mux_link_factorys[rank].get(), rank);
  }
  for (size_t rank = 0; rank < kWorldSize; rank++) {
    futures[rank].wait();
  }
}

}  // namespace scql::engine
