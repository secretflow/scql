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

#include "engine/framework/session_manager.h"

#include <future>
#include <memory>

#include "gtest/gtest.h"
#include "libspu/core/config.h"

#include "engine/framework/session.h"
#include "engine/operator/test_util.h"

namespace scql::engine {

class TestFactory : public yacl::link::ILinkFactory {
 public:
  TestFactory(ListenerManager* listener_manager)
      : listener_manager_(listener_manager) {}

  std::shared_ptr<yacl::link::Context> CreateContext(
      const yacl::link::ContextDesc& desc, size_t self_rank) override {
    auto listener = std::make_shared<Listener>();
    listener_manager_->AddListener(desc.id, listener);
    return mem_link_factory_.CreateContext(desc, self_rank);
  }

 private:
  ListenerManager* listener_manager_;
  static yacl::link::FactoryMem mem_link_factory_;
};

yacl::link::FactoryMem TestFactory::mem_link_factory_;

class SessionManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    factory = std::make_unique<TestFactory>(&listener_manager);
    EXPECT_NE(nullptr, factory.get());
    SessionOptions options;
    std::vector<spu::ProtocolKind> allowed_spu_protocols = {
        spu::ProtocolKind::SEMI2K, spu::ProtocolKind::CHEETAH};
    mgr = std::make_unique<SessionManager>(options, &listener_manager,
                                           std::move(factory), nullptr, nullptr,
                                           1, allowed_spu_protocols);
    EXPECT_NE(nullptr, mgr.get());
  }

 public:
  ListenerManager listener_manager;
  std::unique_ptr<TestFactory> factory;
  std::unique_ptr<SessionManager> mgr;
};

TEST_F(SessionManagerTest, Works) {
  // Given
  std::string session_id = "session_id";
  pb::JobStartParams params;
  {
    params.set_job_id(session_id);

    params.set_party_code(op::test::kPartyAlice);
    auto* alice = params.add_parties();
    alice->CopyFrom(op::test::BuildParty(op::test::kPartyAlice, 0));

    params.mutable_spu_runtime_cfg()->CopyFrom(
        op::test::MakeSpuRuntimeConfigForTest(spu::ProtocolKind::SEMI2K));
  }
  pb::DebugOptions debug_opts;
  // When
  EXPECT_NO_THROW(mgr->CreateSession(params, debug_opts));
  // Then
  EXPECT_NE(nullptr, listener_manager.GetListener(session_id));
  // duplicate creation error.
  EXPECT_THROW(mgr->CreateSession(params, debug_opts), ::yacl::LogicError);
  // GetSession.
  EXPECT_EQ(nullptr, mgr->GetSession("not exist session_id"));
  Session* session = nullptr;
  EXPECT_NO_THROW(session = mgr->GetSession(session_id));
  EXPECT_NE(nullptr, session);
  // SetSessionState
  EXPECT_TRUE(mgr->SetSessionState(session_id, SessionState::INITIALIZED));
  EXPECT_TRUE(mgr->SetSessionState(session_id, SessionState::RUNNING));
  // StopSession/RemoveSession
  EXPECT_THROW(mgr->RemoveSession(session_id), ::yacl::LogicError);
  EXPECT_NO_THROW(mgr->StopSession(session_id));
  EXPECT_TRUE(mgr->SetSessionState(session_id, SessionState::FAILED));
  EXPECT_NO_THROW(mgr->RemoveSession(session_id));
  EXPECT_EQ(nullptr, listener_manager.GetListener(session_id));

  // test timeout.
  session_id = session_id + "_timeout";
  params.set_job_id(session_id);
  EXPECT_NO_THROW(mgr->CreateSession(params, debug_opts));
  EXPECT_TRUE(mgr->SetSessionState(session_id, SessionState::SUCCEEDED));
  EXPECT_NE(nullptr, listener_manager.GetListener(session_id));
  sleep(2);
  EXPECT_EQ(nullptr, listener_manager.GetListener(session_id));
}

TEST_F(SessionManagerTest, TestSessionCreation) {
  pb::JobStartParams common_params;
  common_params.set_job_id("session_multi_pc");
  common_params.add_parties()->CopyFrom(
      op::test::BuildParty(op::test::kPartyAlice, 0));
  common_params.add_parties()->CopyFrom(
      op::test::BuildParty(op::test::kPartyBob, 1));

  yacl::link::FactoryMem g_mem_link_factory;
  SessionOptions options;

  common_params.mutable_spu_runtime_cfg()->CopyFrom(
      op::test::MakeSpuRuntimeConfigForTest(spu::ProtocolKind::REF2K));
  auto create_session = [&](const pb::JobStartParams& params) {
    pb::DebugOptions debug_opts;

    // not allowed to create session with REF2K.
    std::vector<spu::ProtocolKind> allowed_protocols{spu::ProtocolKind::CHEETAH,
                                                     spu::ProtocolKind::SEMI2K,
                                                     spu::ProtocolKind::ABY3};
    EXPECT_THROW(std::make_shared<Session>(options, params, debug_opts,
                                           &g_mem_link_factory, nullptr,
                                           nullptr, allowed_protocols),
                 ::yacl::EnforceNotMet);
  };

  std::vector<std::future<void>> futures;

  pb::JobStartParams alice_params;
  alice_params.CopyFrom(common_params);
  alice_params.set_party_code(op::test::kPartyAlice);
  futures.push_back(std::async(create_session, alice_params));

  pb::JobStartParams bob_params;
  bob_params.CopyFrom(common_params);
  bob_params.set_party_code(op::test::kPartyBob);
  futures.push_back(std::async(create_session, bob_params));

  futures[0].get();
  futures[1].get();
}

}  // namespace scql::engine