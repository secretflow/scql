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

#include "engine/util/ssl_helper.h"

#include "butil/file_util.h"
#include "gtest/gtest.h"
#include "yacl/base/exception.h"

namespace scql::engine::util {

TEST(LoadSslCredentialsOptionsTest, Success) {
  std::string key_file = "key_file";
  std::string cert_file = "cert_file";
  std::string cacert_file = "cacert_file";

  butil::CreateDirectory(butil::FilePath(key_file).DirName(), true);
  butil::WriteFile(butil::FilePath(key_file), "dummy_key_content",
                   std::strlen("dummy_key_content"));
  butil::WriteFile(butil::FilePath(cert_file), "dummy_cert_content",
                   std::strlen("dummy_cert_content"));
  butil::WriteFile(butil::FilePath(cacert_file), "dummy_cacert_content",
                   std::strlen("dummy_cacert_content"));

  grpc::SslCredentialsOptions opts =
      LoadSslCredentialsOptions(key_file, cert_file, cacert_file);

  EXPECT_EQ(opts.pem_private_key, "dummy_key_content");
  EXPECT_EQ(opts.pem_cert_chain, "dummy_cert_content");
  EXPECT_EQ(opts.pem_root_certs, "dummy_cacert_content");
}

TEST(LoadSslCredentialsOptionsTest, ReadFileFailure) {
  std::string invalid_path = "invalid_path";

  EXPECT_THROW(
      LoadSslCredentialsOptions(invalid_path, invalid_path, invalid_path),
      yacl::EnforceNotMet);
}
}  // namespace scql::engine::util