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

#include "engine/operator/all_ops_register.h"

#include "gtest/gtest.h"

namespace scql::engine::op {

TEST(AllOpsRegisterTest, Works) {
  const std::string test_op_type = "FilterByIndex";
  auto op = GetOpRegistry()->GetOperator(test_op_type);
  EXPECT_EQ(op, nullptr);

  EXPECT_NO_THROW(RegisterAllOps());
  EXPECT_NO_THROW(RegisterAllOps());

  op = GetOpRegistry()->GetOperator(test_op_type);
  EXPECT_NE(op, nullptr);
  EXPECT_EQ(test_op_type, op->Type());
}

}  // namespace scql::engine::op