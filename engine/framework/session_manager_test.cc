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

#include "gtest/gtest.h"

#include "engine/framework/session.h"

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
    mgr = std::make_unique<SessionManager>(
        options, &listener_manager, std::move(factory), nullptr, nullptr, 1);
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
  pb::DebugOptions debug_opts;
  pb::SessionStartParams params;
  {
    params.set_session_id(session_id);

    params.set_party_code("alice");
    auto alice = params.add_parties();
    alice->set_code("alice");
    alice->set_name("party alice");
    alice->set_host("alice.com");
    alice->set_rank(0);

    auto config = params.mutable_spu_runtime_cfg();
    config->set_protocol(spu::ProtocolKind::SEMI2K);
    config->set_field(spu::FieldType::FM64);
    config->set_sigmoid_mode(spu::RuntimeConfig::SIGMOID_REAL);
  }
  // When
  EXPECT_NO_THROW(mgr->CreateSession(params, debug_opts));
  // Then
  EXPECT_NE(nullptr, listener_manager.GetListener(session_id));
  // duplicate creation error.
  EXPECT_THROW(mgr->CreateSession(params, debug_opts), ::yacl::LogicError);
  // GetSession.
  EXPECT_EQ(nullptr, mgr->GetSession("not exist session_id"));
  EXPECT_NE(nullptr, mgr->GetSession(session_id));
  // SetSessionState
  EXPECT_TRUE(mgr->SetSessionState(session_id, SessionState::IDLE,
                                   SessionState::RUNNING));
  EXPECT_FALSE(mgr->SetSessionState(session_id, SessionState::IDLE,
                                    SessionState::RUNNING));
  EXPECT_TRUE(mgr->SetSessionState(session_id, SessionState::RUNNING,
                                   SessionState::IDLE));
  // RemoveSession.
  EXPECT_NO_THROW(mgr->RemoveSession(session_id));
  EXPECT_EQ(nullptr, listener_manager.GetListener(session_id));

  // test timeout.
  session_id = session_id + "_timeout";
  params.set_session_id(session_id);
  EXPECT_NO_THROW(mgr->CreateSession(params, debug_opts));
  EXPECT_NE(nullptr, listener_manager.GetListener(session_id));
  sleep(2);
  EXPECT_EQ(nullptr, listener_manager.GetListener(session_id));
}

}  // namespace scql::engine