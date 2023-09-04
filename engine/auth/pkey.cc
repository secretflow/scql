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

#include "engine/auth/pkey.h"

#include <cstdio>

#include "openssl/evp.h"
#include "openssl/pem.h"
#include "yacl/base/exception.h"
#include "yacl/utils/scope_guard.h"

namespace scql::engine::auth {

using UniqueBio = std::unique_ptr<BIO, decltype(&BIO_free)>;

std::unique_ptr<PrivateKey> PrivateKey::ParseFromPem(
    std::string_view pem_content) {
  UniqueBio bio(BIO_new_mem_buf(pem_content.data(), pem_content.size()),
                BIO_free);
  EVP_PKEY* pkey =
      PEM_read_bio_PrivateKey(bio.get(), nullptr, nullptr, nullptr);

  YACL_ENFORCE(pkey != nullptr, "no private key found in pem");

  return std::make_unique<PrivateKey>(pkey, private_ctor_tag{});
}

std::unique_ptr<PrivateKey> PrivateKey::LoadFromPemFile(
    const std::string& file_path) {
  FILE* fp = std::fopen(file_path.c_str(), "r");
  YACL_ENFORCE(fp, "Failed to open file: {}", file_path);

  ON_SCOPE_EXIT([&fp] { std::fclose(fp); });

  EVP_PKEY* pkey = PEM_read_PrivateKey(fp, nullptr, nullptr, nullptr);
  YACL_ENFORCE(pkey, "No private key found in pem file: {}", file_path);
  return std::make_unique<PrivateKey>(pkey, private_ctor_tag{});
}

PrivateKey::PrivateKey(EVP_PKEY* pkey, private_ctor_tag)
    : pkey_(pkey, ::EVP_PKEY_free) {}

std::string PrivateKey::GetPublicKeyInDER() const {
  unsigned char* pkey_der = nullptr;
  int pkey_der_len = i2d_PUBKEY(pkey_.get(), &pkey_der);
  ON_SCOPE_EXIT([&pkey_der] { OPENSSL_free(pkey_der); });
  YACL_ENFORCE(pkey_der_len > 0, "i2d_PUBKEY returns error");
  return std::string(reinterpret_cast<char*>(pkey_der), pkey_der_len);
}

}  // namespace scql::engine::auth