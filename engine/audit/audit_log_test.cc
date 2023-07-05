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

#include "engine/audit/audit_log.h"

#include <google/protobuf/util/message_differencer.h>

#include <cstdio>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <utility>

#include "google/protobuf/util/json_util.h"
#include "gtest/gtest.h"

namespace scql::engine::audit {

class AuditLogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    scql::engine::audit::AuditOptions opt;
    {
      opt.enable_audit_logger = true;
      opt.audit_log_dir = "audit";
    }
    SetupAudit(opt);

    time_t now = time(nullptr);
    tm *ltm = localtime(&now);
    std::ostringstream data_time;
    data_time << std::setw(4) << std::setfill('0') << ltm->tm_year + 1900 << "-"
              << std::setw(2) << std::setfill('0') << ltm->tm_mon + 1 << "-"
              << std::setw(2) << std::setfill('0') << ltm->tm_mday;

    default_file_name_ = "audit/audit_" + data_time.str() + ".log";
    detail_file_name_ = "audit/detail_" + data_time.str() + ".log";
  }

 public:
  std::string default_file_name_;
  std::string detail_file_name_;
};

TEST_F(AuditLogTest, RecordSqlNodeDetail) {
  AuditLog audit;
  auto *header = audit.mutable_header();
  header->mutable_status()->set_code(0);
  header->set_event_name(::audit::pb::SQL_DETAIL);
  header->set_session_id("a0b72d96-f305-11ed-833c-0242c0a82005");
  header->mutable_time()->set_seconds(1664589600);
  auto *detail = audit.mutable_body()->mutable_sql_detail();
  detail->mutable_query()->append("select plain_long_0 from scdb.alice_tbl_1s");
  detail->set_node_name("runsql.0");
  detail->set_num_rows(10);
  detail->set_num_columns(100);
  detail->set_cost_time(20000);
  RecordAudit(audit);

  std::ifstream log_file;
  log_file.open(detail_file_name_, std::ios::in);
  ASSERT_TRUE(log_file.is_open());
  std::string actual;
  getline(log_file, actual);
  log_file.close();

  std::string expected =
      "{\"header\":{\"status\":{\"code\":0,\"message\":\"\",\"details\":[]},"
      "\"event_name\":\"SQL_DETAIL\",\"session_id\":\"a0b72d96-f305-11ed-"
      "833c-0242c0a82005\",\"time\":\"2022-10-01T02:00:00Z\"},\"body\":{\"sql_"
      "detail\":{\"node_name\":\"runsql.0\",\"query\":\"select plain_long_0 "
      "from "
      "scdb.alice_tbl_1s\",\"num_rows\":\"10\",\"num_columns\":\"100\",\"cost_"
      "time\":\"20000\"}}}";
  ASSERT_EQ(actual, expected);
}

TEST_F(AuditLogTest, RunSubDagEvent) {
  AuditLog exceptedAudit;
  auto *header = exceptedAudit.mutable_header();
  header->mutable_status()->set_code(0);
  header->set_event_name(::audit::pb::RUN_SUB_DAG);
  header->set_session_id("a0b72d96-f305-11ed-833c-0242c0a82005");
  header->mutable_time()->set_seconds(1664589600);

  auto *body = exceptedAudit.mutable_body()->mutable_run_dag();
  body->set_dag_id(2);
  ::audit::pb::NodeInfo node_info;

  node_info.set_name("join.2");

  ::google::protobuf::Map<std::string, ::audit::pb::strings> input_info;
  input_info["Left"].add_ss("demo.tb.id.4");
  input_info["Right"].add_ss("demo.tb.id.0");
  ::google::protobuf::Map<std::string, ::audit::pb::strings> output_info;
  output_info["LeftJoinIndex"].add_ss("demo.tb.id.7");
  output_info["RightJoinIndex"].add_ss("demo.tb.id.8");

  node_info.mutable_inputs()->swap(input_info);
  node_info.mutable_outputs()->swap(output_info);

  body->add_node_list()->CopyFrom(node_info);

  RecordAudit(exceptedAudit);
  std::ifstream log_file;
  log_file.open(default_file_name_, std::ios::in);
  ASSERT_TRUE(log_file.is_open());
  std::string log_str;
  getline(log_file, log_str);
  log_file.close();

  AuditLog actualAudit;
  auto status =
      ::google::protobuf::util::JsonStringToMessage(log_str, &actualAudit);
  ASSERT_TRUE(status.ok());

  ASSERT_TRUE(::google::protobuf::util::MessageDifferencer::Equals(
      actualAudit, exceptedAudit));
}

}  // namespace scql::engine::audit
