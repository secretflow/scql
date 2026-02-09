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

#include "engine/datasource/dp_adaptor.h"

#include <gtest/gtest.h>

#include "arrow/flight/client.h"
#include "arrow/table.h"
#include "google/protobuf/util/json_util.h"

using namespace testing;

namespace scql::engine {

std::string GenerateValidJson() {
  return R"json({"dp_uri":"grpc+tcp://localhost:8989","datasource":{"type":"odps","info":{"odps":{"endpoint":"xxx","project":"xxx","accessKeyId":"xxx","accessKeySecret":"xxx"}}}})json";
}

TEST(DpAdaptorTest, Constructor) {
  std::string json = GenerateValidJson();
  // No exceptions should be thrown during construction
  EXPECT_NO_THROW({ DpAdaptor dp_adaptor(json); });

  std::string invalid_json = R"({"invalid_field": "missing_dp_uri"})";
  EXPECT_THROW({ DpAdaptor dp_adaptor(invalid_json); }, yacl::EnforceNotMet);
}

TEST(DpAdaptorTest, OtherFunc) {
  std::string json = GenerateValidJson();
  DpAdaptor dp_adaptor(json);
  std::vector<ColumnDesc> outputs{{"age", scql::pb::PrimitiveDataType::INT64}};
  EXPECT_THROW(
      {
        auto results =
            dp_adaptor.ExecQuery("SELECT age from user_credit;", outputs);
      },
      yacl::RuntimeError);
}

}  // namespace scql::engine
