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

#include "engine/util/filepath_helper.h"

#include <filesystem>
#include <string>

#include "gtest/gtest.h"
#include "yacl/base/exception.h"

namespace scql::engine::util {

TEST(FilePathTest, CheckAndGetAbsolutePath) {
  std::string path = "/data/test.file";
  EXPECT_EQ(path, CheckAndGetAbsolutePath(path, false, "null"));

  EXPECT_EQ(path, CheckAndGetAbsolutePath(path, true, "/data"));
  EXPECT_THROW(CheckAndGetAbsolutePath(path, true, "/another_dir"),
               yacl::EnforceNotMet);
  EXPECT_THROW(CheckAndGetAbsolutePath(path, true, ""), yacl::EnforceNotMet);

  std::string current_path = std::filesystem::current_path().string();
  EXPECT_EQ(current_path + "/test.file",
            CheckAndGetAbsolutePath("./test.file", false, ""));

  EXPECT_EQ(current_path + "/data/test.file",
            CheckAndGetAbsolutePath("./data/test.file", true, "./data"));
  EXPECT_THROW(
      CheckAndGetAbsolutePath("./data/../other_dir/test.file", true, "./data"),
      yacl::EnforceNotMet);
  EXPECT_THROW(
      CheckAndGetAbsolutePath("../other_dir/test.file", true, "./data"),
      yacl::EnforceNotMet);

  // test s3 file
  EXPECT_EQ("bucket/dir/test.file",
            CheckAndGetAbsolutePath("bucket/dir/test.file", true, "bucket"));
}

}  // namespace scql::engine::util
