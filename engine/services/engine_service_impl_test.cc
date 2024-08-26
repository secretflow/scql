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

#include "engine/services/engine_service_impl.h"

#include <cstdint>

#include "Poco/Data/SQLite/Connector.h"
#include "Poco/Data/Session.h"
#include "brpc/server.h"
#include "gtest/gtest.h"
#include "spdlog/spdlog.h"

#include "engine/framework/session.h"
#include "engine/link/mux_link_factory.h"
#include "engine/link/mux_receiver_service.h"
#include "engine/operator/filter_by_index.h"
#include "engine/operator/join.h"
#include "engine/operator/publish.h"
#include "engine/operator/run_sql.h"
#include "engine/operator/test_util.h"
#include "engine/util/psi_helper.h"

#include "api/status_code.pb.h"
#include "engine/services/mock_report_service.pb.h"

namespace scql::engine {

class MockReportServiceImpl : public services::pb::MockReportService {
 public:
  void Report(::google::protobuf::RpcController* controller,
              const pb::ReportRequest* request,
              services::pb::MockResponse* response,
              ::google::protobuf::Closure* done) override {
    brpc::ClosureGuard done_guard(done);
    req_id = request->job_id();
    status.CopyFrom(request->status());
    if (request->out_columns_size() > 0) {
      first_tensor = request->out_columns(0);
    }
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    cntl->response_attachment().append("receive report succ.");
  }

 public:
  std::string req_id;
  pb::Status status;
  pb::Tensor first_tensor;
};

class EngineServiceImplTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Construct EngineServiceImpl.
    factory =
        std::make_unique<MuxLinkFactory>(&channel_manager, &listener_manager);
    EXPECT_NE(nullptr, factory.get());
    engine_service_options.enable_authorization = true;
    engine_service_options.credential = "alice_credential";
    std::vector<spu::ProtocolKind> allowed_protocols = {
        spu::ProtocolKind::SEMI2K};
    impl = std::make_unique<EngineServiceImpl>(
        engine_service_options,
        std::make_unique<SessionManager>(session_options, &listener_manager,
                                         std::move(factory), nullptr, nullptr,
                                         1, allowed_protocols),
        &channel_manager, nullptr);
    EXPECT_NE(nullptr, impl.get());
    {
      global_session_id = "test_session_id";
      // prepare pb::JobStartParams global_params.
      global_params.set_job_id(global_session_id);

      global_params.set_party_code(op::test::kPartyAlice);
      auto* alice = global_params.add_parties();
      alice->CopyFrom(op::test::BuildParty(op::test::kPartyAlice, 0));
      global_cntl.http_request().SetHeader(
          "Credential", fmt::format("{}_credential", op::test::kPartyAlice));

      global_params.mutable_spu_runtime_cfg()->CopyFrom(
          op::test::MakeSpuRuntimeConfigForTest(spu::ProtocolKind::SEMI2K));
    }
  }

  void CheckJobStatus(pb::JobState pb_state) {
    pb::QueryJobStatusRequest request;
    request.set_job_id(global_session_id);
    pb::QueryJobStatusResponse response;
    EXPECT_NO_THROW(
        impl->QueryJobStatus(&global_cntl, &request, &response, nullptr));
    EXPECT_EQ(pb::Code::OK, response.status().code());
    EXPECT_EQ(pb_state, response.job_state());
  }

  void SetAndCheckJobStatus(SessionState state) {
    auto* session_manager = impl->GetSessionManager();
    EXPECT_TRUE(session_manager->SetSessionState(global_session_id, state));

    CheckJobStatus(ConvertSessionStateToJobState(state));
  }

  void CheckJobNodeCount(int32_t expected_nodes_count,
                         int32_t expected_executed_nodes) {
    pb::QueryJobStatusRequest request;
    request.set_job_id(global_session_id);
    pb::QueryJobStatusResponse response;
    EXPECT_NO_THROW(
        impl->QueryJobStatus(&global_cntl, &request, &response, nullptr));
    EXPECT_EQ(pb::Code::OK, response.status().code());
    EXPECT_EQ(expected_nodes_count, response.progress().stages_count());
    EXPECT_EQ(expected_executed_nodes, response.progress().executed_stages());
  }

 public:
  ListenerManager listener_manager;
  ChannelManager channel_manager;
  std::unique_ptr<MuxLinkFactory> factory;
  std::unique_ptr<EngineServiceImpl> impl;
  std::string global_session_id;
  brpc::Controller global_cntl;
  pb::JobStartParams global_params;
  SessionOptions session_options;
  EngineServiceOptions engine_service_options;
};

TEST_F(EngineServiceImplTest, QueryJobStatus) {
  // Given
  //  bypass RunExecutionPlan, control the session's state manually.
  pb::DebugOptions debug_opts;
  pb::JobStartParams params;
  {
    params.set_job_id(global_session_id);

    params.set_party_code(op::test::kPartyAlice);
    auto* alice = params.add_parties();
    alice->CopyFrom(op::test::BuildParty(op::test::kPartyAlice, 0));

    params.mutable_spu_runtime_cfg()->CopyFrom(
        op::test::MakeSpuRuntimeConfigForTest(spu::ProtocolKind::SEMI2K));
  }

  // When
  auto* session_manager = impl->GetSessionManager();
  EXPECT_NO_THROW(session_manager->CreateSession(params, debug_opts));
  // Then
  CheckJobStatus(pb::JOB_INITIALIZED);
  // When & Then
  SetAndCheckJobStatus(SessionState::RUNNING);
  SetAndCheckJobStatus(SessionState::ABORTING);
  SetAndCheckJobStatus(SessionState::SUCCEEDED);
  SetAndCheckJobStatus(SessionState::FAILED);

  CheckJobNodeCount(-1, -1);
  auto* session = session_manager->GetSession(global_session_id);
  const int32_t nodes_count = 10;
  const int32_t executed_nodes = 5;
  session->SetNodesCount(nodes_count);
  session->SetExecutedNodes(executed_nodes);
  CheckJobNodeCount(nodes_count, executed_nodes);
}

TEST_F(EngineServiceImplTest, RunExecutionPlan) {
  // Given
  pb::RunExecutionPlanRequest request;
  request.mutable_job_params()->CopyFrom(global_params);
  pb::RunExecutionPlanResponse response;
  // When
  EXPECT_NO_THROW(
      impl->RunExecutionPlan(&global_cntl, &request, &response, nullptr));
  // Then
  EXPECT_EQ(pb::Code::OK, response.status().code());

  // test with ExecNode
  pb::ExecNode node;
  node.set_node_name("Publish.0");
  node.set_op_type("Publish");
  std::string node_id = "0";
  auto* graph = request.mutable_graph();
  (*graph->mutable_nodes())[node_id] = node;
  auto* pipeline = graph->mutable_policy()->add_pipelines();
  auto* subdag = pipeline->add_subdags();
  auto* job = subdag->add_jobs();
  job->add_node_ids(node_id);

  EXPECT_NO_THROW(
      impl->RunExecutionPlan(&global_cntl, &request, &response, nullptr));
  EXPECT_EQ(pb::Code::UNKNOWN_ENGINE_ERROR, response.status().code());
}

TEST_F(EngineServiceImplTest, RunExecutionPlanAsync) {
  // Given
  brpc::Server recv_server;
  MockReportServiceImpl service;
  {
    ASSERT_EQ(
        0, recv_server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE));
    brpc::ServerOptions recv_options;
    ASSERT_EQ(0, recv_server.Start("127.0.0.1:0", &recv_options));
  }
  pb::RunExecutionPlanRequest request;
  request.mutable_job_params()->CopyFrom(global_params);
  request.set_async(true);
  request.set_callback_url(
      fmt::format("{}/MockReportService/Report",
                  butil::endpoint2str(recv_server.listen_address()).c_str()));

  pb::RunExecutionPlanResponse response;
  // When
  EXPECT_NO_THROW(
      impl->RunExecutionPlan(&global_cntl, &request, &response, nullptr));
  // Then
  EXPECT_EQ(pb::Code::OK, response.status().code());

  usleep(100 * 1000);  // wait async run finished.
  EXPECT_EQ(global_session_id, service.req_id);
  EXPECT_EQ(pb::Code::OK, service.status.code());

  // test with error response
  pb::ExecNode node;
  node.set_node_name("Publish.0");
  node.set_op_type("Publish");
  std::string node_id = "0";
  auto* graph = request.mutable_graph();
  (*graph->mutable_nodes())[node_id] = node;
  auto* pipeline = graph->mutable_policy()->add_pipelines();
  auto* subdag = pipeline->add_subdags();
  auto* job = subdag->add_jobs();
  job->add_node_ids(node_id);

  EXPECT_NO_THROW(
      impl->RunExecutionPlan(&global_cntl, &request, &response, nullptr));
  EXPECT_EQ(pb::Code::OK, response.status().code());

  usleep(100 * 1000);  // wait async run finished.
  EXPECT_EQ(global_session_id, service.req_id);
  EXPECT_EQ(pb::Code::UNKNOWN_ENGINE_ERROR, service.status.code());

  recv_server.Stop(1000);
  recv_server.Join();
}

// ===========================Test for 2 Parties =========================

struct InnerJoinTestCase {
  std::vector<std::string> alice;
  std::vector<std::string> bob;
  std::vector<std::string> inner_join_result;
};
class EngineServiceImpl2PartiesTest
    : public ::testing::TestWithParam<InnerJoinTestCase> {
 protected:
  void SetUp() override {
    // Start Brpc Receive Services
    servers = std::vector<brpc::Server>(kWorldSize);
    for (size_t rank = 0; rank < kWorldSize; rank++) {
      listener_managers.emplace_back(new ListenerManager());
      services.emplace_back(
          new MuxReceiverServiceImpl(listener_managers[rank].get()));
      ASSERT_EQ(0, servers[rank].AddService(services[rank].get(),
                                            brpc::SERVER_DOESNT_OWN_SERVICE));

      brpc::ServerOptions options;
      ASSERT_EQ(0, servers[rank].Start("127.0.0.1:0", &options));
    }

    global_cntl.http_request().SetHeader("Credential", "alice_credential");
    // Construct EngineServiceImpl
    for (size_t rank = 0; rank < kWorldSize; rank++) {
      auto factory = std::make_unique<MuxLinkFactory>(
          &channel_manager, listener_managers[rank].get());
      ASSERT_NE(nullptr, factory.get());
      EngineServiceOptions service_options;
      service_options.enable_authorization = true;
      service_options.credential = "alice_credential";
      SessionOptions session_options;
      std::vector<spu::ProtocolKind> allowed_protocols = {
          spu::ProtocolKind::SEMI2K};
      auto impl = std::make_unique<EngineServiceImpl>(
          service_options,
          std::make_unique<SessionManager>(
              session_options, listener_managers[rank].get(),
              std::move(factory), EmbedRouter::FromJsonStr(router_json_str),
              std::make_unique<DatasourceAdaptorMgr>(), 10, allowed_protocols),
          &channel_manager, nullptr);
      ASSERT_NE(nullptr, impl.get());

      factories.push_back(std::move(factory));
      engine_svcs.emplace_back(std::move(impl));
    }
    ASSERT_EQ(kWorldSize, engine_svcs.size());
  }

  static std::unique_ptr<Poco::Data::Session> PrepareTableInMemory(
      const InnerJoinTestCase& tc, const std::string& db_connection_str);

  static pb::RunExecutionPlanRequest ConstructRequestForAlice(
      const std::vector<brpc::Server>& servers);

  static pb::RunExecutionPlanRequest ConstructRequestForBob(
      const std::vector<brpc::Server>& servers);

  static void AddSessionParameters(pb::RunExecutionPlanRequest* request,
                                   const std::vector<brpc::Server>& servers,
                                   const size_t& self_rank);

  static void AddRunSQLNode(pb::RunExecutionPlanRequest* request,
                            const std::string& table_name,
                            const std::string& out_name, int ref_count);

  static void AddJoinNode(pb::RunExecutionPlanRequest* request,
                          const std::pair<std::string, std::string>& in_name,
                          const std::pair<std::string, std::string>& out_name,
                          int ref_count);

  static void AddFilterByIndexNode(pb::RunExecutionPlanRequest* request,
                                   const std::string& in_name,
                                   const std::string& indice_name,
                                   const std::string& out_name, int ref_count);

  static void AddPublishNode(pb::RunExecutionPlanRequest* request,
                             const std::string& in_name,
                             const std::string& out_name, int ref_count);

 protected:
  const size_t kWorldSize = 2u;
  std::vector<std::unique_ptr<ListenerManager>> listener_managers;
  std::vector<std::unique_ptr<MuxReceiverServiceImpl>> services;
  std::vector<brpc::Server> servers;
  ChannelManager channel_manager;
  std::vector<std::unique_ptr<MuxLinkFactory>> factories;
  std::vector<std::unique_ptr<EngineServiceImpl>> engine_svcs;
  brpc::Controller global_cntl;
  const std::string router_json_str = R"json({
      "datasources": [
        {
          "id": "ds001",
          "name": "sqlite3",
          "kind": "SQLITE",
          "connection_str": "file:runsql_test?mode=memory&cache=shared"
        }
      ],
      "rules":[
        {
          "db": "*",
          "table": "*",
          "datasource_id": "ds001"
        }
      ]
    })json";
};

INSTANTIATE_TEST_SUITE_P(
    InnerJoinTest, EngineServiceImpl2PartiesTest,
    testing::Values(InnerJoinTestCase{.alice = {"alice", "bob", "carol"},
                                      .bob = {"alice", "carol"},
                                      .inner_join_result = {"alice", "carol"}},
                    InnerJoinTestCase{
                        .alice = {"B", "B", "D"},
                        .bob = {"B", "B"},
                        .inner_join_result = {"B", "B", "B", "B"}},
                    InnerJoinTestCase{.alice = {"B", "B", "D"},
                                      .bob = {"A", "C"},
                                      .inner_join_result = {}}));

// run the case: find persons who both exist in ta(Table A) and tb(Table B).
TEST_P(EngineServiceImpl2PartiesTest, RunExecutionPlan) {
  // Given
  auto test_case = GetParam();
  auto session = PrepareTableInMemory(
      test_case, "file:runsql_test?mode=memory&cache=shared");

  // When
  auto proc = [&](EngineServiceImpl* svc, pb::RunExecutionPlanRequest* request,
                  pb::RunExecutionPlanResponse* response) {
    EXPECT_NO_THROW(
        svc->RunExecutionPlan(&global_cntl, request, response, nullptr));
    EXPECT_EQ(pb::Code::OK, response->status().code());
  };

  pb::RunExecutionPlanResponse response_alice;
  pb::RunExecutionPlanRequest request_alice = ConstructRequestForAlice(servers);
  auto future_alice =
      std::async(proc, engine_svcs[0].get(), &request_alice, &response_alice);

  pb::RunExecutionPlanResponse response_bob;
  pb::RunExecutionPlanRequest request_bob = ConstructRequestForBob(servers);
  auto future_bob =
      std::async(proc, engine_svcs[1].get(), &request_bob, &response_bob);
  // Then
  EXPECT_NO_THROW(future_alice.wait());
  SPDLOG_INFO("out: \n{}", response_alice.DebugString());

  EXPECT_NO_THROW(future_bob.wait());
  SPDLOG_INFO("out: \n{}", response_bob.DebugString());

  auto check_equal = [](const pb::Tensor& actual_result,
                        std::vector<std::string>& expect_result) {
    std::vector<std::string> tmp;
    tmp.reserve(actual_result.string_data_size());
    for (auto item : actual_result.string_data()) {
      tmp.push_back(item);
    }
    std::sort(tmp.begin(), tmp.end());
    std::sort(expect_result.begin(), expect_result.end());
    EXPECT_EQ(tmp.size(), expect_result.size());

    for (size_t i = 0; i < tmp.size(); ++i) {
      EXPECT_EQ(tmp[i], expect_result[i]);
    }
  };
  ASSERT_EQ(response_alice.out_columns_size(), 1);
  check_equal(response_alice.out_columns(0), test_case.inner_join_result);
  ASSERT_EQ(response_bob.out_columns_size(), 1);
  check_equal(response_bob.out_columns(0), test_case.inner_join_result);

  // -------test async run---------
  {
    // start mock report service.
    brpc::Server recv_server;
    MockReportServiceImpl service;
    ASSERT_EQ(
        0, recv_server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE));
    brpc::ServerOptions recv_options;
    ASSERT_EQ(0, recv_server.Start("127.0.0.1:0", &recv_options));
    // modify request
    request_alice.set_async(true);
    request_alice.set_callback_url(
        fmt::format("{}/MockReportService/Report",
                    butil::endpoint2str(recv_server.listen_address()).c_str()));
    request_bob.set_async(true);
    // only check alice result
    request_bob.set_callback_url(
        fmt::format("{}/MockReportService/NotExistReport",
                    butil::endpoint2str(recv_server.listen_address()).c_str()));

    pb::RunExecutionPlanResponse response_alice;
    auto future_alice =
        std::async(proc, engine_svcs[0].get(), &request_alice, &response_alice);

    pb::RunExecutionPlanResponse response_bob;
    auto future_bob =
        std::async(proc, engine_svcs[1].get(), &request_bob, &response_bob);

    EXPECT_NO_THROW(future_alice.wait());
    EXPECT_NO_THROW(future_bob.wait());

    ASSERT_EQ(response_alice.status().code(), 0);
    ASSERT_EQ(response_bob.status().code(), 0);

    usleep(100 * 1000);  // wait report
    ASSERT_EQ(service.req_id, "test_session_id");
    ASSERT_EQ(service.status.code(), 0);
    check_equal(service.first_tensor, test_case.inner_join_result);

    recv_server.Stop(1000);
    recv_server.Join();
  }
}

/// ===========================
/// Test for 2 Parties Implementation
/// ===========================

std::unique_ptr<Poco::Data::Session>
EngineServiceImpl2PartiesTest::PrepareTableInMemory(
    const InnerJoinTestCase& tc, const std::string& db_connection_str) {
  Poco::Data::SQLite::Connector::registerConnector();

  auto result =
      std::make_unique<Poco::Data::Session>("SQLite", db_connection_str);

  using Poco::Data::Keywords::now;
  // create table: ta
  *result << "CREATE TABLE ta(name VARCHAR(30))", now;
  // insert some rows
  for (size_t i = 0; i < tc.alice.size(); ++i) {
    std::string row = fmt::format("INSERT INTO ta VALUES(\"{}\")", tc.alice[i]);
    *result << row, now;
  }

  // create table: tb
  *result << "CREATE TABLE tb(name VARCHAR(30))", now;
  // insert some rows
  for (size_t i = 0; i < tc.bob.size(); ++i) {
    std::string row = fmt::format("INSERT INTO tb VALUES(\"{}\")", tc.bob[i]);
    *result << row, now;
  }

  return result;
}

pb::RunExecutionPlanRequest
EngineServiceImpl2PartiesTest::ConstructRequestForAlice(
    const std::vector<brpc::Server>& servers) {
  pb::RunExecutionPlanRequest request;

  // set graph checksum
  auto* checker = request.mutable_graph_checksum();
  checker->set_check_graph_checksum(true);
  checker->mutable_sub_graph_checksums()->insert({"0", "test"});
  checker->mutable_sub_graph_checksums()->insert({"1", "test"});
  checker->mutable_whole_graph_checksum()->append("whole");
  AddSessionParameters(&request, servers, 0);

  AddRunSQLNode(&request, "ta", "ta.name", 2);

  AddJoinNode(&request, {"ta.name", "tb.name"}, {"ta.index", "tb.index"}, 1);

  AddFilterByIndexNode(&request, "ta.name", "ta.index", "ta.filtered", 1);

  AddPublishNode(&request, "ta.filtered", "name", 0);

  return request;
}

pb::RunExecutionPlanRequest
EngineServiceImpl2PartiesTest::ConstructRequestForBob(
    const std::vector<brpc::Server>& servers) {
  pb::RunExecutionPlanRequest request;

  // set graph checksum
  auto* checker = request.mutable_graph_checksum();
  checker->set_check_graph_checksum(true);
  checker->mutable_sub_graph_checksums()->insert({"0", "test"});
  checker->mutable_sub_graph_checksums()->insert({"1", "test"});
  checker->mutable_whole_graph_checksum()->append("whole");

  AddSessionParameters(&request, servers, 1);

  AddRunSQLNode(&request, "tb", "tb.name", 2);

  AddJoinNode(&request, {"ta.name", "tb.name"}, {"ta.index", "tb.index"}, 1);

  AddFilterByIndexNode(&request, "tb.name", "tb.index", "tb.filtered", 1);

  AddPublishNode(&request, "tb.filtered", "name", 0);

  return request;
}

void EngineServiceImpl2PartiesTest::AddSessionParameters(
    pb::RunExecutionPlanRequest* request,
    const std::vector<brpc::Server>& servers, const size_t& self_rank) {
  auto* params = request->mutable_job_params();
  params->set_job_id("test_session_id");
  params->set_party_code("party" + std::to_string(self_rank));
  for (size_t rank = 0; rank < servers.size(); rank++) {
    auto party = params->add_parties();
    party->set_code("party" + std::to_string(rank));
    party->set_name(party->code());
    party->set_host(
        butil::endpoint2str(servers[rank].listen_address()).c_str());
    party->set_rank(rank);
  }

  params->mutable_spu_runtime_cfg()->CopyFrom(
      op::test::MakeSpuRuntimeConfigForTest(spu::ProtocolKind::SEMI2K));
}

void EngineServiceImpl2PartiesTest::AddRunSQLNode(
    pb::RunExecutionPlanRequest* request, const std::string& table_name,
    const std::string& out_name, int ref_count) {
  const std::string query = "SELECT name FROM " + table_name;
  const std::vector<std::string> table_refs = {"test.test"};
  op::test::ExecNodeBuilder node_builder(op::RunSQL::kOpType);
  node_builder.SetNodeName("runsql-test");
  node_builder.AddStringAttr(op::RunSQL::kSQLAttr, query);
  node_builder.AddStringsAttr(op::RunSQL::kTableRefsAttr, table_refs);
  auto out = op::test::MakeTensorReference(
      out_name, pb::PrimitiveDataType::STRING,
      pb::TensorStatus::TENSORSTATUS_PRIVATE, ref_count);
  node_builder.AddOutput(op::RunSQL::kOut, {out});
  auto node = node_builder.Build();
  auto* graph = request->mutable_graph();
  (*(graph->mutable_nodes()))[op::RunSQL::kOpType] = node;
  auto* pipeline = graph->mutable_policy()->add_pipelines();
  auto* subdag = pipeline->add_subdags();
  auto* job = subdag->add_jobs();
  job->add_node_ids(op::RunSQL::kOpType);
}

void EngineServiceImpl2PartiesTest::AddJoinNode(
    pb::RunExecutionPlanRequest* request,
    const std::pair<std::string, std::string>& in_name,
    const std::pair<std::string, std::string>& out_name, int ref_count) {
  op::test::ExecNodeBuilder builder(op::Join::kOpType);
  builder.SetNodeName("join-test");
  builder.AddInt64Attr(op::Join::kJoinTypeAttr, util::kInnerJoin);
  builder.AddInt64Attr(op::Join::kAlgorithmAttr,
                       static_cast<int64_t>(util::PsiAlgo::kEcdhPsi));
  builder.AddStringsAttr(op::Join::kInputPartyCodesAttr,
                         std::vector<std::string>{"party0", "party1"});
  auto [in_name_left, in_name_right] = in_name;
  auto in_left =
      op::test::MakeTensorReference(in_name_left, pb::PrimitiveDataType::STRING,
                                    pb::TensorStatus::TENSORSTATUS_PRIVATE);
  auto in_right = op::test::MakeTensorReference(
      in_name_right, pb::PrimitiveDataType::STRING,
      pb::TensorStatus::TENSORSTATUS_PRIVATE);
  builder.AddInput(op::Join::kInLeft, {in_left});
  builder.AddInput(op::Join::kInRight, {in_right});
  auto [out_name_left, out_name_right] = out_name;
  auto join_output_left = op::test::MakeTensorReference(
      out_name_left, pb::PrimitiveDataType::INT64,
      pb::TensorStatus::TENSORSTATUS_PRIVATE, ref_count);
  auto join_output_right = op::test::MakeTensorReference(
      out_name_right, pb::PrimitiveDataType::INT64,
      pb::TensorStatus::TENSORSTATUS_PRIVATE, ref_count);
  builder.AddOutput(op::Join::kOutLeftJoinIndex, {join_output_left});
  builder.AddOutput(op::Join::kOutRightJoinIndex, {join_output_right});

  auto node = builder.Build();
  auto* graph = request->mutable_graph();
  (*(graph->mutable_nodes()))[op::Join::kOpType] = node;
  auto* pipeline = graph->mutable_policy()->add_pipelines();
  auto* subdag = pipeline->add_subdags();
  auto* job = subdag->add_jobs();
  job->add_node_ids(op::Join::kOpType);
}

void EngineServiceImpl2PartiesTest::AddFilterByIndexNode(
    pb::RunExecutionPlanRequest* request, const std::string& in_name,
    const std::string& indice_name, const std::string& out_name,
    int ref_count) {
  op::test::ExecNodeBuilder builder(op::FilterByIndex::kOpType);
  builder.SetNodeName("filter-by-index-test");
  auto indice =
      op::test::MakeTensorReference(indice_name, pb::PrimitiveDataType::INT64,
                                    pb::TensorStatus::TENSORSTATUS_PRIVATE);
  builder.AddInput(op::FilterByIndex::kInRowsIndexFilter, {indice});
  auto in =
      op::test::MakeTensorReference(in_name, pb::PrimitiveDataType::STRING,
                                    pb::TensorStatus::TENSORSTATUS_PRIVATE);
  builder.AddInput(op::FilterByIndex::kInData, {in});
  auto out = op::test::MakeTensorReference(
      out_name, pb::PrimitiveDataType::STRING,
      pb::TensorStatus::TENSORSTATUS_PRIVATE, ref_count);
  builder.AddOutput(op::FilterByIndex::kOut, {out});

  auto node = builder.Build();
  auto* graph = request->mutable_graph();
  (*(graph->mutable_nodes()))[op::FilterByIndex::kOpType] = node;
  auto pipeline = graph->mutable_policy()->add_pipelines();
  auto* subdag = pipeline->add_subdags();
  auto* job = subdag->add_jobs();
  job->add_node_ids(op::FilterByIndex::kOpType);
}

void EngineServiceImpl2PartiesTest::AddPublishNode(
    pb::RunExecutionPlanRequest* request, const std::string& in_name,
    const std::string& out_name, int ref_count) {
  op::test::ExecNodeBuilder builder(op::Publish::kOpType);
  builder.SetNodeName("publish-test");
  auto in =
      op::test::MakeTensorReference(in_name, pb::PrimitiveDataType::STRING,
                                    pb::TensorStatus::TENSORSTATUS_PRIVATE);
  builder.AddInput(op::Publish::kIn, {in});

  pb::Tensor out;
  out.set_name(out_name);
  out.set_elem_type(pb::PrimitiveDataType::STRING);
  out.set_option(pb::TensorOptions::VALUE);
  out.add_string_data(out_name);
  builder.AddOutput(op::Publish::kOut, {out});

  auto node = builder.Build();
  auto* graph = request->mutable_graph();
  (*(graph->mutable_nodes()))[op::Publish::kOpType] = node;
  auto pipeline = graph->mutable_policy()->add_pipelines();
  auto* subdag = pipeline->add_subdags();
  auto* job = subdag->add_jobs();
  job->add_node_ids(op::Publish::kOpType);
}

}  // namespace scql::engine