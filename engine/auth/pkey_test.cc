#include "engine/auth/pkey.h"

#include <sstream>

#include "absl/strings/escaping.h"
#include "butil/files/temp_file.h"
#include "butil/popen.h"
#include "fmt/format.h"
#include "gtest/gtest.h"

using scql::engine::auth::PrivateKey;

class PrivateKeyTest : public ::testing::Test {};

TEST_F(PrivateKeyTest, LoadFromPemFileWorks) {
  // create private key pem
  butil::TempFile pem_file;
  std::string cmd = fmt::format("openssl genpkey -algorithm ed25519 -out {}",
                                pem_file.fname());
  std::stringstream ss;
  int ret = butil::read_command_output(ss, cmd.c_str());
  ASSERT_EQ(ret, 0) << "failed to generate private key using openssl";

  std::unique_ptr<PrivateKey> priv_key;
  EXPECT_NO_THROW(
      { priv_key = PrivateKey::LoadFromPemFile(pem_file.fname()); });
  EXPECT_TRUE(priv_key != nullptr);
}

TEST_F(PrivateKeyTest, ParseFromPemWorks) {
  std::string pem_content = R"pem(-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEINTZWUEn/Jt6TV9OxGxjD+6CtqKB3MtcJdFAzFUg3fk/
-----END PRIVATE KEY-----
)pem";
  std::string expect_public_key =
      "MCowBQYDK2VwAyEAPBDjfKgiUSIjVLrvsR+pxw5i9unTpr8S5BL04T13r6w=";

  std::unique_ptr<PrivateKey> priv_key;
  EXPECT_NO_THROW({ priv_key = PrivateKey::ParseFromPem(pem_content); });
  EXPECT_TRUE(priv_key != nullptr);
  EXPECT_EQ(absl::Base64Escape(priv_key->GetPublicKeyInDER()),
            expect_public_key);
}