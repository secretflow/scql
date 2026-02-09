// Copyright 2025 Ant Group Co., Ltd.
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

#include "engine/util/kpad_task_helper.h"

#include "gtest/gtest.h"

namespace scql::engine::util {
TEST(KpadTaskHelperTest, ParseClusterDefSuccess) {
  const std::string cluster_def_json = R"({
    "self_party": "alice",
    "parties": ["alice", "bob"],
    "services": {
      "alice": {
        "job123": {
          "name": "job123",
          "host": "localhost",
          "port": "8080",
          "scope": "cluster",
          "protocol": "http"
        }
      }
    }
  })";

  auto cluster_def = ParseClusterDef(cluster_def_json);
  EXPECT_NE(cluster_def, nullptr);
  EXPECT_EQ(cluster_def->self_party, "alice");
  EXPECT_EQ(cluster_def->parties.size(), 2);
  EXPECT_EQ(cluster_def->parties[0], "alice");
  EXPECT_EQ(cluster_def->parties[1], "bob");
}

TEST(KpadTaskHelperTest, ParseClusterDefFailure) {
  const std::string invalid_json = "{ invalid json }";

  auto cluster_def = ParseClusterDef(invalid_json);
  EXPECT_EQ(cluster_def, nullptr);
}

TEST(KpadTaskHelperTest, ParseScqlConfigSuccess) {
  const std::string scql_config_json = R"({
    "input_file_paths": {
      "db.table1": "/path/to/table1.csv",
      "db.table2": "/path/to/table2.csv"
    },
    "parties": ["alice", "bob"],
    "time_zone": "+08:00"
  })";

  auto scql_config = ParseScqlConfig(scql_config_json);
  EXPECT_NE(scql_config, nullptr);
  EXPECT_EQ(scql_config->input_file_paths_size(), 2);
  EXPECT_EQ(scql_config->input_file_paths().at("db.table1"),
            "/path/to/table1.csv");
  EXPECT_EQ(scql_config->input_file_paths().at("db.table2"),
            "/path/to/table2.csv");
  EXPECT_EQ(scql_config->parties_size(), 2);
  EXPECT_EQ(scql_config->parties(0), "alice");
  EXPECT_EQ(scql_config->parties(1), "bob");
  EXPECT_EQ(scql_config->time_zone(), "+08:00");
}

TEST(KpadTaskHelperTest, ParseScqlConfigFailure) {
  const std::string invalid_json = "{ invalid json }";

  auto scql_config = ParseScqlConfig(invalid_json);
  EXPECT_EQ(scql_config, nullptr);
}

TEST(KpadTaskHelperTest, GetPartyHostSuccess) {
  ClusterDef cluster_def;
  cluster_def.self_party = "alice";

  Service service1;
  service1.name = "job1";
  service1.host = "localhost";
  service1.port = "8080";
  cluster_def.services["alice"]["job1"] = service1;

  // Test: Should return first service regardless of job_id
  std::string result = GetPartyHost(cluster_def, "alice", "nonexistent_job");
  EXPECT_EQ(result, "localhost");
}

TEST(KpadTaskHelperTest, GetPartyHostPartyFailure) {
  ClusterDef cluster_def;
  cluster_def.self_party = "alice";

  Service service;
  service.name = "job1";
  service.host = "localhost";
  service.port = "8080";
  cluster_def.services["alice"]["job1"] = service;

  // Test: Non-existent party should return empty string
  std::string result = GetPartyHost(cluster_def, "bob", "job1");
  EXPECT_EQ(result, "");
}

TEST(KpadTaskHelperTest, BuildJobStartParamsSuccess) {
  ClusterDef cluster_def;
  cluster_def.self_party = "alice";

  Service service;
  service.name = "job1";
  service.host = "localhost";
  service.port = "8080";
  cluster_def.services["alice"]["job1"] = service;

  Service bob_service;
  bob_service.name = "job1";
  bob_service.host = "localhost";
  bob_service.port = "8081";
  cluster_def.services["bob"]["job1"] = bob_service;

  scql::pb::ScqlConfig scql_config;
  scql_config.add_parties("alice");
  scql_config.add_parties("bob");

  auto job_params = BuildJobStartParams(cluster_def, scql_config, "job1");
  EXPECT_NE(job_params, nullptr);
  EXPECT_EQ(job_params->job_id(), "job1");
  EXPECT_EQ(job_params->party_code(), "alice");
  EXPECT_EQ(job_params->parties_size(), 2);
  EXPECT_EQ(job_params->parties(0).code(), "alice");
  EXPECT_EQ(job_params->parties(0).host(), "localhost");
  EXPECT_EQ(job_params->parties(1).code(), "bob");
  EXPECT_EQ(job_params->parties(1).host(), "localhost");
}

TEST(KpadTaskHelperTest, BuildJobStartParamsFailure) {
  ClusterDef cluster_def;
  cluster_def.self_party = "alice";

  // Only add alice service, not bob
  Service service;
  service.name = "job1";
  service.host = "localhost";
  service.port = "8080";
  cluster_def.services["alice"]["job1"] = service;

  scql::pb::ScqlConfig scql_config;
  scql_config.add_parties("alice");
  scql_config.add_parties("bob");  // bob is missing from services

  auto job_params = BuildJobStartParams(cluster_def, scql_config, "job1");
  EXPECT_EQ(job_params, nullptr);
}

struct ValidateParamsTestCase {
  std::string cluster_def;
  std::string job_id;
  std::string scql_config;
  bool expected;
  std::string description;
};

class ValidateKpadTaskParamsTest
    : public testing::TestWithParam<ValidateParamsTestCase> {};

TEST_P(ValidateKpadTaskParamsTest, Validate) {
  const auto& tc = GetParam();
  EXPECT_EQ(ValidateKpadTaskParams(tc.cluster_def, tc.job_id, tc.scql_config),
            tc.expected)
      << "Failed for case: " << tc.description;
}

INSTANTIATE_TEST_SUITE_P(
    KpadTaskHelper, ValidateKpadTaskParamsTest,
    testing::Values(
        ValidateParamsTestCase{"", "", "", true, "all empty (broker mode)"},
        ValidateParamsTestCase{"cluster_def", "job_id", "scql_config", true,
                               "all non-empty (kpad mode)"},
        ValidateParamsTestCase{"", "job_id", "scql_config", false,
                               "missing cluster_def"},
        ValidateParamsTestCase{"cluster_def", "", "scql_config", false,
                               "missing job_id"},
        ValidateParamsTestCase{"cluster_def", "job_id", "", false,
                               "missing scql_config"},
        ValidateParamsTestCase{"cluster_def", "", "", false,
                               "only cluster_def"},
        ValidateParamsTestCase{"", "job_id", "", false, "only job_id"},
        ValidateParamsTestCase{"", "", "scql_config", false,
                               "only scql_config"}));

}  // namespace scql::engine::util
