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

#include "engine/operator/if_null.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct IfNullTestCase {
  test::NamedTensor exp;
  test::NamedTensor alt;
  test::NamedTensor expect_out;
};

class IfNullTest : public testing::TestWithParam<
                       std::tuple<test::SpuRuntimeTestCase, IfNullTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const IfNullTestCase& tc);
  static void FeedInputs(ExecContext* ctx, const IfNullTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    IfNullBatchTest, IfNullTest,
    testing::Combine(
        testing::Values(test::SpuRuntimeTestCase{spu::ProtocolKind::SEMI2K, 2}),
        testing::Values(
            // test private status
            IfNullTestCase{
                .exp = test::NamedTensor(
                    "exp", TensorFrom(arrow::int64(), "[null, 2, 3, null]")),
                .alt = test::NamedTensor(
                    "alt", TensorFrom(arrow::int64(), "[null, 12, 13, 14]")),
                .expect_out = test::NamedTensor(
                    "out", TensorFrom(arrow::int64(), "[null, 2, 3, 14]"))},
            IfNullTestCase{
                .exp = test::NamedTensor("exp",
                                         TensorFrom(arrow::float64(),
                                                    "[null, -0.1, 1.1, null]")),
                .alt = test::NamedTensor(
                    "alt",
                    TensorFrom(arrow::float64(), "[null, -10.1, 11.1, 12.1]")),
                .expect_out = test::NamedTensor(
                    "out",
                    TensorFrom(arrow::float64(), "[null, -0.1, 1.1, 12.1]"))},
            IfNullTestCase{
                .exp = test::NamedTensor(
                    "exp", TensorFrom(arrow::large_utf8(),
                                      R"json(["B", null, null ,"B"])json")),
                .alt = test::NamedTensor(
                    "alt", TensorFrom(arrow::large_utf8(),
                                      R"json(["CC", null, "CC" ,"C"])json")),
                .expect_out = test::NamedTensor(
                    "out", TensorFrom(arrow::large_utf8(),
                                      R"json(["B", null, "CC" ,"B"])json"))},
            IfNullTestCase{
                .exp = test::NamedTensor("exp",
                                         TensorFrom(arrow::float64(), "[]")),
                .alt = test::NamedTensor("alt",
                                         TensorFrom(arrow::float64(), "[]")),
                .expect_out = test::NamedTensor(
                    "out", TensorFrom(arrow::float64(), "[]"))})),
    TestParamNameGenerator(IfNullTest));

TEST_P(IfNullTest, works) {
  // Given
  auto parm = GetParam();
  auto tc = std::get<1>(parm);
  auto node = MakeExecNode(tc);
  auto session = test::Make1PCSession();
  ExecContext ctx(node, session.get());

  FeedInputs(&ctx, tc);

  // When
  IfNull op;
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
/// IfNullTest impl
/// ===================

pb::ExecNode IfNullTest::MakeExecNode(const IfNullTestCase& tc) {
  test::ExecNodeBuilder builder(IfNull::kOpType);
  builder.SetNodeName("if-null-test");

  {
    auto t =
        test::MakePrivateTensorReference(tc.exp.name, tc.exp.tensor->Type());
    builder.AddInput(IfNull::kExpr, {t});
  }
  {
    auto t =
        test::MakePrivateTensorReference(tc.alt.name, tc.alt.tensor->Type());
    builder.AddInput(IfNull::kAltValue, {t});
  }

  {
    auto t = test::MakePrivateTensorReference(tc.expect_out.name,
                                              tc.expect_out.tensor->Type());
    builder.AddOutput(IfNull::kOut, {t});
  }

  return builder.Build();
}

void IfNullTest::FeedInputs(ExecContext* ctx, const IfNullTestCase& tc) {
  test::FeedInputsAsPrivate(ctx, {tc.exp, tc.alt});
}

}  // namespace scql::engine::op