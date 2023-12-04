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

#include "engine/link/rpc_helper.h"

#include "brpc/server.h"
#include "gtest/gtest.h"

#include "engine/link/mux_receiver.pb.h"

// disable detect leaks for brpc's "acceptable mem leak"
// https://github.com/apache/incubator-brpc/blob/0.9.6/src/brpc/server.cpp#L1138
extern "C" const char* __asan_default_options() { return "detect_leaks=0"; }

namespace scql::engine {

namespace {

// brpc receiver service for test.
class RecvTestImpl : public link::pb::MuxReceiverService {
 public:
  void Push(::google::protobuf::RpcController* cntl,
            const link::pb::MuxPushRequest* request,
            link::pb::MuxPushResponse* response,
            ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    if (request->link_id() == "") {
      response->set_error_code(link::pb::ErrorCode::INVALID_REQUEST);
      response->set_error_msg("no link_id.");
      return;
    }

    response->set_error_code(link::pb::ErrorCode::SUCCESS);
    response->set_error_msg("no error.");
  }
};

static SimpleAuthenticator g_my_auth("test");
}  // namespace

class RpcHelperTest : public testing::Test {
 public:
  void SetUp() override {
    service_.reset(new RecvTestImpl);
    ASSERT_EQ(
        server_.AddService(service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE), 0)
        << "Fail to add service";

    brpc::ServerOptions server_options;
    server_options.auth = &g_my_auth;
    ASSERT_EQ(server_.Start("127.0.0.1:0", &server_options), 0)
        << "Fail to start server at 127.0.0.1:0";
    host_ = butil::endpoint2str(server_.listen_address()).c_str();
  }

 protected:
  brpc::Server server_;
  std::unique_ptr<RecvTestImpl> service_;
  std::string host_;
};

TEST_F(RpcHelperTest, RpcNoAuth) {
  brpc::ChannelOptions options;
  brpc::Controller cntl;
  brpc::Channel channel;

  options.protocol = "baidu_std";
  ASSERT_EQ(channel.Init(host_.c_str(), &options), 0);
  link::pb::MuxReceiverService::Stub stub(&channel);

  link::pb::MuxPushRequest request;
  request.set_link_id("link_id");
  link::pb::MuxPushResponse response;

  stub.Push(&cntl, &request, &response, nullptr);

  EXPECT_TRUE(cntl.Failed());
  EXPECT_EQ(EHOSTDOWN, cntl.ErrorCode());
}

TEST_F(RpcHelperTest, RpcWithRetryAndAuth) {
  brpc::ChannelOptions options;
  brpc::Controller cntl;
  brpc::Channel channel;

  options.protocol = "baidu_std";
  options.auth = &g_my_auth;
  ASSERT_EQ(channel.Init(host_.c_str(), &options), 0);
  link::pb::MuxReceiverService::Stub stub(&channel);

  link::pb::MuxPushRequest request;
  request.set_link_id("link_id");
  link::pb::MuxPushResponse response;

  stub.Push(&cntl, &request, &response, nullptr);

  EXPECT_FALSE(cntl.Failed());
  EXPECT_EQ(response.error_code(), link::pb::ErrorCode::SUCCESS);
}

}  // namespace scql::engine