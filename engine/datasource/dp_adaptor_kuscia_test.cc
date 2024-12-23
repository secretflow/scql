// Copyright 2024 Ant Group Co., Ltd.
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

#include <gflags/gflags_declare.h>

#include <iostream>

#include "absl/debugging/failure_signal_handler.h"
#include "absl/debugging/symbolize.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_split.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"

#include "engine/datasource/dp_adaptor.h"

DEFINE_string(domaindata_id, "", "domain data id split by ','");

DECLARE_string(kuscia_datamesh_endpoint);
DECLARE_string(kuscia_datamesh_client_key_path);
DECLARE_string(kuscia_datamesh_client_cert_path);
DECLARE_string(kuscia_datamesh_cacert_path);

using namespace scql::engine;

int main(int argc, char* argv[]) {
  {
    absl::InitializeSymbolizer(argv[0]);

    absl::FailureSignalHandlerOptions options;
    absl::InstallFailureSignalHandler(options);
  }

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // Given
  DpAdaptor adaptor(FLAGS_domaindata_id);
  // When
  const std::string query = "SELECT count(*) as tt FROM table;";
  std::vector<ColumnDesc> outputs{{"tt", scql::pb::PrimitiveDataType::INT64}};

  auto results = adaptor.ExecQuery(query, outputs);
  std::cout << "results.size: " << results.size() << '\n';
  std::cout << "results length: " << results[0]->Length() << '\n';
  std::cout << "results null count: " << results[0]->GetNullCount() << '\n';
  std::cout << "results to string: "
            << results[0]->ToArrowChunkedArray()->ToString() << '\n';
  return 0;
}