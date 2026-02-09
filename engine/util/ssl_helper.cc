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
#include "yacl/base/exception.h"

namespace scql::engine::util {

grpc::SslCredentialsOptions LoadSslCredentialsOptions(
    const std::string& key_file, const std::string& cert_file,
    const std::string& cacert_file) {
  grpc::SslCredentialsOptions opts;

  std::string content;
  YACL_ENFORCE(butil::ReadFileToString(butil::FilePath(cacert_file), &content));
  opts.pem_root_certs = content;

  YACL_ENFORCE(butil::ReadFileToString(butil::FilePath(key_file), &content));
  opts.pem_private_key = content;

  YACL_ENFORCE(butil::ReadFileToString(butil::FilePath(cert_file), &content));
  opts.pem_cert_chain = content;
  return opts;
}

}  // namespace scql::engine::util