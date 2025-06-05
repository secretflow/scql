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

#include "engine/operator/str_to_date.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct StrToDateTestCase {
  test::NamedTensor date_str_input;
  test::NamedTensor format_str_input;
  test::NamedTensor expect_out;
};

class StrToDateTest
    : public testing::TestWithParam<
          std::tuple<test::SpuRuntimeTestCase, StrToDateTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const StrToDateTestCase& tc);
  static void FeedInputs(ExecContext* ctx, const StrToDateTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    StrToDateBatchTest, StrToDateTest,
    testing::Combine(
        testing::Values(test::SpuRuntimeTestCase{spu::ProtocolKind::SEMI2K, 2}),
        testing::Values(
            StrToDateTestCase{
                .date_str_input = test::NamedTensor(
                    "date_str",
                    TensorFrom(arrow::large_utf8(),
                               R"json(["2023-01-15", "2024-12-30"])json")),
                .format_str_input = test::NamedTensor(
                    "format_str",
                    TensorFrom(arrow::large_utf8(), R"json(["%Y-%m-%d"])json")),
                .expect_out = test::NamedTensor(
                    "out_timestamp",
                    TensorFrom(arrow::int64(),
                               R"json([1673740800, 1735516800])json"))},
            StrToDateTestCase{
                .date_str_input = test::NamedTensor(
                    "date_str",
                    TensorFrom(arrow::large_utf8(),
                               R"json(["2023-03-20 10:30:45"])json")),
                .format_str_input = test::NamedTensor(
                    "format_str",
                    TensorFrom(arrow::large_utf8(),
                               R"json(["%Y-%m-%d %H:%M:%S"])json")),
                .expect_out = test::NamedTensor(
                    "out_timestamp",
                    TensorFrom(arrow::int64(), R"json([1679308245])json"))},
            StrToDateTestCase{
                .date_str_input = test::NamedTensor(
                    "date_str",
                    TensorFrom(arrow::large_utf8(),
                               R"json(["invalid-date", "2023-05-10"])json")),
                .format_str_input = test::NamedTensor(
                    "format_str",
                    TensorFrom(arrow::large_utf8(), R"json(["%Y-%m-%d"])json")),
                .expect_out = test::NamedTensor(
                    "out_timestamp",
                    TensorFrom(arrow::int64(),
                               R"json([null, 1683676800])json"))})),
    TestParamNameGenerator(StrToDateTest));

TEST_P(StrToDateTest, works) {
  // Given
  auto parm = GetParam();
  auto tc = std::get<1>(parm);
  auto node = MakeExecNode(tc);
  auto session = test::Make1PCSession();
  ExecContext ctx(node, session.get());

  FeedInputs(&ctx, tc);

  StrToDate op;
  ASSERT_NO_THROW(op.Run(&ctx));

  TensorPtr result_tensor =
      session->GetTensorTable()->GetTensor(tc.expect_out.name);
  ASSERT_TRUE(result_tensor);

  auto result_arrow_array = result_tensor->ToArrowChunkedArray();
  auto expect_arrow_array = tc.expect_out.tensor->ToArrowChunkedArray();

  ASSERT_TRUE(result_arrow_array->type()->Equals(expect_arrow_array->type()))
      << "Output type mismatch: expected "
      << expect_arrow_array->type()->ToString() << ", got "
      << result_arrow_array->type()->ToString();

  EXPECT_TRUE(result_arrow_array->Equals(expect_arrow_array))
      << "Output content mismatch:"
      << "\nExpected: " << expect_arrow_array->ToString()
      << "\nGot: " << result_arrow_array->ToString();
}

pb::ExecNode StrToDateTest::MakeExecNode(const StrToDateTestCase& tc) {
  test::ExecNodeBuilder builder(tc.date_str_input.name + "_strtodate_test");

  {
    auto t_ref = test::MakePrivateTensorReference(
        tc.date_str_input.name, tc.date_str_input.tensor->Type());
    builder.AddInput(StrToDate::kLeft, {t_ref});
  }

  {
    auto t_ref = test::MakePrivateTensorReference(
        tc.format_str_input.name, tc.format_str_input.tensor->Type());
    builder.AddInput(StrToDate::kRight, {t_ref});
  }

  {
    auto t_ref = test::MakePrivateTensorReference(tc.expect_out.name,
                                                  tc.expect_out.tensor->Type());
    builder.AddOutput(StrToDate::kOut, {t_ref});
  }

  return builder.Build();
}

void StrToDateTest::FeedInputs(ExecContext* ctx, const StrToDateTestCase& tc) {
  test::FeedInputsAsPrivate(ctx, {tc.date_str_input, tc.format_str_input});
}

}  // namespace scql::engine::op