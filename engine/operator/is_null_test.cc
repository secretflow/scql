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

#include "engine/operator/is_null.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct IsNullTestCase {
  test::NamedTensor input;
  test::NamedTensor expect_out;
};

class IsNullTest : public testing::TestWithParam<
                       std::tuple<test::SpuRuntimeTestCase, IsNullTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const IsNullTestCase& tc);
  static void FeedInputs(ExecContext* ctx, const IsNullTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    IsNullBatchTest, IsNullTest,
    testing::Combine(
        testing::Values(test::SpuRuntimeTestCase{spu::ProtocolKind::SEMI2K, 2}),
        testing::Values(
            // test private status
            IsNullTestCase{
                .input = test::NamedTensor(
                    "in", TensorFrom(arrow::int64(), "[null, 2, 3, null]")),
                .expect_out = test::NamedTensor(
                    "out", TensorFrom(arrow::boolean(),
                                      "[true, false, false, true]"))},
            IsNullTestCase{
                .input = test::NamedTensor(
                    "in",
                    TensorFrom(arrow::float64(), "[null, -0.1, 1.1, null]")),
                .expect_out = test::NamedTensor(
                    "out", TensorFrom(arrow::boolean(),
                                      "[true, false, false, true]"))},
            IsNullTestCase{
                .input = test::NamedTensor(
                    "in", TensorFrom(arrow::large_utf8(),
                                     R"json(["B", null, null ,"B"])json")),
                .expect_out = test::NamedTensor(
                    "out", TensorFrom(arrow::boolean(),
                                      "[false, true, true, false]"))},
            IsNullTestCase{.input = test::NamedTensor(
                               "in", TensorFrom(arrow::float64(), "[]")),
                           .expect_out = test::NamedTensor(
                               "out", TensorFrom(arrow::boolean(), "[]"))})),
    TestParamNameGenerator(IsNullTest));

TEST_P(IsNullTest, works) {
  // Given
  auto parm = GetParam();
  auto tc = std::get<1>(parm);
  auto node = MakeExecNode(tc);
  auto session = test::Make1PCSession();
  ExecContext ctx(node, session.get());

  FeedInputs(&ctx, tc);

  // When
  IsNull op;
  ASSERT_NO_THROW(op.Run(&ctx));

  // Then
  // check alice output
  TensorPtr t = session->GetTensorTable()->GetTensor(tc.expect_out.name);
  ASSERT_TRUE(t);
  auto out_arr = t->ToArrowChunkedArray();

  auto expect_arr = tc.expect_out.tensor->ToArrowChunkedArray();

  // compare tensor content
  EXPECT_TRUE(out_arr->Equals(expect_arr))
      << "expect type = " << expect_arr->type()->ToString()
      << ", got type = " << out_arr->type()->ToString()
      << "\nexpect result = " << expect_arr->ToString()
      << "\nbut actual got result = " << out_arr->ToString();
}

/// ===================
/// IsNullTest impl
/// ===================

pb::ExecNode IsNullTest::MakeExecNode(const IsNullTestCase& tc) {
  test::ExecNodeBuilder builder(IsNull::kOpType);
  builder.SetNodeName("is-null-test");

  {
    auto t = test::MakePrivateTensorReference(tc.input.name,
                                              tc.input.tensor->Type());
    builder.AddInput(IsNull::kIn, {t});
  }

  {
    auto t = test::MakePrivateTensorReference(tc.expect_out.name,
                                              tc.expect_out.tensor->Type());
    builder.AddOutput(IsNull::kOut, {t});
  }

  return builder.Build();
}

void IsNullTest::FeedInputs(ExecContext* ctx, const IsNullTestCase& tc) {
  test::FeedInputsAsPrivate(ctx, {tc.input});
}

}  // namespace scql::engine::op