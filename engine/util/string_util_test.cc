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

#include "engine/util/string_util.h"

#include "gtest/gtest.h"
#include "yacl/base/exception.h"

namespace scql::engine::util {

TEST(MySQLDateFormatToArrowFormatTest, ValidFormatCases) {
  EXPECT_EQ(MySQLDateFormatToArrowFormat("%Y-%m-%d"), "%Y-%m-%d");
  EXPECT_EQ(MySQLDateFormatToArrowFormat("%Y-%m-%d %H:%i:%s"),
            "%Y-%m-%d %H:%M:%S");
  EXPECT_EQ(MySQLDateFormatToArrowFormat("%r"), "%I:%M:%S %p");
  EXPECT_EQ(MySQLDateFormatToArrowFormat("%T"), "%H:%M:%S");
  EXPECT_EQ(MySQLDateFormatToArrowFormat("%M %d, %Y"), "%B %d, %Y");
  EXPECT_EQ(MySQLDateFormatToArrowFormat("%Y/%c/%e"), "%Y/%-m/%-d");
  EXPECT_EQ(MySQLDateFormatToArrowFormat("%a, %b %d"), "%a, %b %d");
  EXPECT_EQ(MySQLDateFormatToArrowFormat("2024-%m-%d"), "2024-%m-%d");
  EXPECT_EQ(MySQLDateFormatToArrowFormat("%%"), "%");
}

TEST(MySQLDateFormatToArrowFormatTest, InvalidFormatSpecifier) {
  EXPECT_THROW(MySQLDateFormatToArrowFormat("%x-%m-%d"), yacl::EnforceNotMet);
  EXPECT_THROW(MySQLDateFormatToArrowFormat("%Y-%m-%"), yacl::EnforceNotMet);
  EXPECT_THROW(MySQLDateFormatToArrowFormat("%"), yacl::EnforceNotMet);
}

}  // namespace scql::engine::util