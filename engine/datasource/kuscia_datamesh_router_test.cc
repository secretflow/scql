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

#include "engine/datasource/kuscia_datamesh_router.h"

#include "absl/debugging/failure_signal_handler.h"
#include "absl/debugging/symbolize.h"
#include "butil/file_util.h"
#include "gflags/gflags.h"
#include "yacl/base/exception.h"

DEFINE_string(kuscia_datamesh_endpoint, "", "kuscia datamesh server endpoint");
DEFINE_string(domaindata_id, "", "domain data id");
DEFINE_string(ca_cert, "", "");
DEFINE_string(key, "", "");
DEFINE_string(cert, "", "");

using namespace scql::engine;

grpc::SslCredentialsOptions read_credential_options() {
  grpc::SslCredentialsOptions opts;
  std::string content;
  YACL_ENFORCE(
      butil::ReadFileToString(butil::FilePath(FLAGS_ca_cert), &content));
  opts.pem_root_certs = content;
  YACL_ENFORCE(butil::ReadFileToString(butil::FilePath(FLAGS_key), &content));
  opts.pem_private_key = content;
  YACL_ENFORCE(butil::ReadFileToString(butil::FilePath(FLAGS_cert), &content));
  opts.pem_cert_chain = content;
  return opts;
}

int main(int argc, char* argv[]) {
  {
    absl::InitializeSymbolizer(argv[0]);

    absl::FailureSignalHandlerOptions options;
    absl::InstallFailureSignalHandler(options);
  }

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto ssl_opts = read_credential_options();
  auto channel_creds = grpc::SslCredentials(ssl_opts);
  KusciaDataMeshRouter router(FLAGS_kuscia_datamesh_endpoint, channel_creds);

  try {
    auto datasources =
        router.Route(std::vector<std::string>{FLAGS_domaindata_id});
    for (const auto& datasource : datasources) {
      std::cout << "Datasource: " << datasource.ShortDebugString() << std::endl;
    }
  } catch (const std::exception& e) {
    std::cerr << "Something bad happend: " << e.what() << std::endl;
  }
  return 0;
}