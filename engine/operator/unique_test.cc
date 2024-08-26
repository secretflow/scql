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

#include "engine/operator/unique.h"

#include "arrow/type.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct UniqueTestCase {
  test::NamedTensor input;
  test::NamedTensor expect_out;
};

class UniqueTest : public testing::TestWithParam<UniqueTestCase> {
 protected:
  static pb::ExecNode MakeExecNode(const UniqueTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    UniqueBatchTest, UniqueTest,
    testing::Values(
        UniqueTestCase{
            .input = test::NamedTensor(
                "a", TensorFrom(arrow::boolean(),
                                "[true, false, true, null, false, null]")),
            .expect_out = test::NamedTensor(
                "a_out", TensorFrom(arrow::boolean(), "[true, false, null]"))},
        UniqueTestCase{
            .input = test::NamedTensor(
                "a", TensorFrom(arrow::int64(), "[1,1,2,3,3,4,4,4,null,null]")),
            .expect_out = test::NamedTensor(
                "a_out", TensorFrom(arrow::int64(), "[1,2,3,4,null]"))},
        UniqueTestCase{
            .input = test::NamedTensor(
                "a", TensorFrom(arrow::float32(),
                                "[-10.2, 3.14, 0.34, 3.14,null,null]")),
            .expect_out = test::NamedTensor(
                "a_out",
                TensorFrom(arrow::float32(), "[-10.2, 3.14, 0.34,null]"))},
        UniqueTestCase{.input = test::NamedTensor(
                           "a", TensorFrom(arrow::large_utf8(),
                                           R"json(["AB","AB",null,null])json")),
                       .expect_out = test::NamedTensor(
                           "a_out", TensorFrom(arrow::large_utf8(),
                                               R"json(["AB",null])json"))},
        UniqueTestCase{
            .input = test::NamedTensor("a", TensorFrom(arrow::int64(), "[]")),
            .expect_out = test::NamedTensor("a_out", TensorFrom(arrow::int64(),
                                                                "[]"))}));

TEST_P(UniqueTest, works) {
  // Given
  auto tc = GetParam();
  auto node = MakeExecNode(tc);
  auto session = test::Make1PCSession();
  ExecContext ctx(node, session.get());

  test::FeedInputsAsPrivate(&ctx, {tc.input});

  // When
  Unique op;
  EXPECT_NO_THROW(op.Run(&ctx));

  // Then
  auto expect_arr = tc.expect_out.tensor->ToArrowChunkedArray();
  auto out = ctx.GetTensorTable()->GetTensor(tc.expect_out.name);
  ASSERT_TRUE(out);
  auto out_arr = out->ToArrowChunkedArray();
  // compare tensor content
  EXPECT_TRUE(out_arr->ApproxEquals(*expect_arr))
      << "expect type = " << expect_arr->type()->ToString()
      << ", got type = " << out_arr->type()->ToString()
      << "\nexpect result = " << expect_arr->ToString()
      << "\nbut actual got result = " << out_arr->ToString();
}

/// ===================
/// UniqueTest impl
/// ===================

pb::ExecNode UniqueTest::MakeExecNode(const UniqueTestCase& tc) {
  test::ExecNodeBuilder builder(Unique::kOpType);

  builder.SetNodeName("unique-test");

  {
    auto t = test::MakePrivateTensorReference(tc.input.name,
                                              tc.input.tensor->Type());
    builder.AddInput(Unique::kIn, {t});
  }

  {
    auto t = test::MakePrivateTensorReference(tc.expect_out.name,
                                              tc.expect_out.tensor->Type());
    builder.AddOutput(Unique::kOut, {t});
  }

  return builder.Build();
}

}  // namespace scql::engine::op