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

#include "engine/link/mux_link_factory.h"

#include <algorithm>
#include <vector>

#include "brpc/server.h"
#include "gtest/gtest.h"
#include "spdlog/spdlog.h"

#include "engine/link/mux_receiver.pb.h"
namespace scql::engine {

// brpc receiver service for test.
class RecvTestImpl : public link::pb::MuxReceiverService {
 public:
  void Push(::google::protobuf::RpcController* cntl,
            const link::pb::MuxPushRequest* request,
            link::pb::MuxPushResponse* response,
            ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    {
      std::lock_guard<std::mutex> guard(lock_);
      last_req.CopyFrom(*request);
      if (request->msg().trans_type() == link::pb::TransType::CHUNKED) {
        chunk_msgs.emplace_back(request->msg().chunk_info().chunk_offset(),
                                request->msg().value());
      }
    }
    response->set_error_code(link::pb::ErrorCode::SUCCESS);
    response->set_error_msg("");
  }

 public:
  link::pb::MuxPushRequest last_req;
  std::vector<std::pair<size_t, std::string>> chunk_msgs;

 private:
  std::mutex lock_;
};

TEST(MuxLinkFactoryTest, Works) {
  size_t self_rank = 0;
  size_t peer_rank = 1;
  // start server for peer_rank to receive msg from self_rank.
  brpc::Server recv_server;
  RecvTestImpl service;
  ASSERT_EQ(0,
            recv_server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE));
  brpc::ServerOptions recv_options;
  ASSERT_EQ(0, recv_server.Start("127.0.0.1:0", &recv_options));
  // create context for self_rank.
  yacl::link::ContextDesc link_desc;
  link_desc.http_max_payload_size = 10;
  link_desc.parties.push_back(
      {fmt::format("party{}", self_rank), "null_self_rank_host"});
  const std::string peer_rank_host =
      butil::endpoint2str(recv_server.listen_address()).c_str();
  link_desc.parties.push_back(
      {fmt::format("party{}", peer_rank), peer_rank_host});

  ListenerManager listener_manager;
  ChannelManager channel_manager;
  MuxLinkFactory mux_link_factory(&channel_manager, &listener_manager);
  std::shared_ptr<yacl::link::Context> lc0;
  // When
  EXPECT_NO_THROW(lc0 = mux_link_factory.CreateContext(link_desc, self_rank));
  // Then
  EXPECT_NE(nullptr, lc0);
  auto listener = listener_manager.GetListener(link_desc.id);
  EXPECT_NE(nullptr, listener);
  std::string key = "key";
  std::string value = "0 -> 1.";
  EXPECT_NO_THROW(listener->OnMessage(peer_rank, key, value));
  // When
  EXPECT_NO_THROW(lc0->Send(peer_rank, value, key));
  // Then
  EXPECT_EQ(link_desc.id, service.last_req.link_id());
  EXPECT_EQ(value, service.last_req.msg().value());

  recv_server.Stop(0);
  recv_server.Join();
}

class MuxLinkChannelTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // start server
    ASSERT_EQ(
        0, recv_server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE));
    brpc::ServerOptions recv_options;
    ASSERT_EQ(0, recv_server.Start("127.0.0.1:0", &recv_options));
    // create send channel
    std::string recv_addr =
        butil::endpoint2str(recv_server.listen_address()).c_str();
    SPDLOG_INFO("brpc server address: {}", recv_addr);
    send_channel = std::make_shared<brpc::Channel>();
    const auto load_balancer = "";
    brpc::ChannelOptions send_options;
    ASSERT_EQ(
        0, send_channel->Init(recv_addr.c_str(), load_balancer, &send_options));
    mux_link_channel = std::make_shared<MuxLinkChannel>(
        self_rank, peer_rank, http_max_payload_size, link_id, send_channel);
  }

  void TearDown() override {
    recv_server.Stop(0);
    recv_server.Join();
  }

 public:
  brpc::Server recv_server;
  RecvTestImpl service;
  std::shared_ptr<brpc::Channel> send_channel;
  std::shared_ptr<MuxLinkChannel> mux_link_channel;
  size_t self_rank = 0;
  size_t peer_rank = 1;
  size_t http_max_payload_size = 10;
  std::string link_id = "link_id";
};

TEST_F(MuxLinkChannelTest, SyncSend) {
  // Given
  std::string key = "key";
  std::string value = "value";
  // When: test SendImpl
  ASSERT_NO_THROW(mux_link_channel->Send(key, value));
  // Then
  EXPECT_EQ(link_id, service.last_req.link_id());
  EXPECT_EQ(key, service.last_req.msg().key());
  EXPECT_EQ(value, service.last_req.msg().value());

  // When: test SendChunked
  key = "chunk-key";
  value = "long value for chunk test.";
  ASSERT_NO_THROW(mux_link_channel->Send(key, value));
  // Then
  std::sort(service.chunk_msgs.begin(), service.chunk_msgs.end(),
            [](const std::pair<size_t, std::string>& left,
               const std::pair<size_t, std::string>& right) {
              return left.first < right.first;
            });
  std::string result;
  for (auto item : service.chunk_msgs) {
    result += item.second;
  }
  EXPECT_EQ(key, service.last_req.msg().key());
  EXPECT_EQ(value, result);
}

TEST_F(MuxLinkChannelTest, AsyncSend) {
  // Given
  std::string key = "key";
  std::string value = "value";
  // When: test SendImpl
  ASSERT_NO_THROW(mux_link_channel->SendAsync(key, value));
  ASSERT_NO_THROW(mux_link_channel->WaitAsyncSendToFinish());
  // Then
  EXPECT_EQ(link_id, service.last_req.link_id());
  EXPECT_EQ(key, service.last_req.msg().key());
  EXPECT_EQ(value, service.last_req.msg().value());

  // When: test SendChunked
  key = "chunk-key";
  value = "long value for chunk test.";
  ASSERT_NO_THROW(mux_link_channel->SendAsync(key, value));
  ASSERT_NO_THROW(mux_link_channel->WaitAsyncSendToFinish());
  // Then
  std::sort(service.chunk_msgs.begin(), service.chunk_msgs.end(),
            [](const std::pair<size_t, std::string>& left,
               const std::pair<size_t, std::string>& right) {
              return left.first < right.first;
            });
  std::string result;
  for (auto item : service.chunk_msgs) {
    result += item.second;
  }
  EXPECT_EQ(key, service.last_req.msg().key());
  EXPECT_EQ(value, result);
}

}  // namespace scql::engine
