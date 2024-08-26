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

#include "engine/operator/group_he_sum.h"

#include "arrow/type.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct GroupHeSumTestCase {
  test::NamedTensor group_id;
  test::NamedTensor group_num;
  test::NamedTensor input;
  test::NamedTensor output;
};

class GroupHeSumTest
    : public testing::TestWithParam<
          std::tuple<test::SpuRuntimeTestCase, GroupHeSumTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const GroupHeSumTestCase& tc);
};

TEST_P(GroupHeSumTest, works) {
  // Given
  auto parm = GetParam();
  auto tc = std::get<1>(parm);
  pb::ExecNode node = MakeExecNode(tc);
  auto sessions = test::MakeMultiPCSession(std::get<0>(parm));

  ExecContext alice_ctx(node, sessions[0].get());
  ExecContext bob_ctx(node, sessions[1].get());

  test::FeedInputsAsPrivate(&alice_ctx, {tc.group_id, tc.group_num});
  test::FeedInputsAsPrivate(&bob_ctx, {tc.input});

  // When
  EXPECT_NO_THROW(test::RunAsync<GroupHeSum>({&alice_ctx, &bob_ctx}));

  // Then

  auto actual_output = bob_ctx.GetTensorTable()->GetTensor(tc.output.name);
  ASSERT_TRUE(actual_output);
  auto actual_arr = actual_output->ToArrowChunkedArray();
  auto expect_arr = tc.output.tensor->ToArrowChunkedArray();
  EXPECT_TRUE(actual_arr->ApproxEquals(
      *expect_arr, arrow::EqualOptions::Defaults().atol(0.01)))
      << "\nexpect result = " << expect_arr->ToString()
      << "\nbut actual got result = " << actual_arr->ToString();
}

/// ===================
/// GroupHeSumTest impl
/// ===================

pb::ExecNode GroupHeSumTest::MakeExecNode(const GroupHeSumTestCase& tc) {
  test::ExecNodeBuilder builder(GroupHeSum::kOpType);

  builder.SetNodeName("group-heu-sum-test");
  builder.AddStringsAttr(
      GroupHeSum::kInputPartyCodesAttr,
      std::vector<std::string>{test::kPartyAlice, test::kPartyBob});

  pb::Tensor t_id = test::MakePrivateTensorReference(
      tc.group_id.name, tc.group_id.tensor->Type());
  builder.AddInput(GroupHeSum::kGroupId, {t_id});

  pb::Tensor t_num = test::MakePrivateTensorReference(
      tc.group_num.name, tc.group_num.tensor->Type());
  builder.AddInput(GroupHeSum::kGroupNum, {t_num});

  pb::Tensor t_in =
      test::MakePrivateTensorReference(tc.input.name, tc.input.tensor->Type());
  builder.AddInput(GroupHeSum::kIn, {t_in});

  pb::Tensor t_out = test::MakePrivateTensorReference(tc.output.name,
                                                      tc.output.tensor->Type());
  builder.AddOutput(GroupHeSum::kOut, {t_out});

  return builder.Build();
}

INSTANTIATE_TEST_SUITE_P(
    GroupBatchTest, GroupHeSumTest,
    testing::Combine(
        test::SpuTestValues2PC,
        testing::Values(
            GroupHeSumTestCase{
                .group_id = test::NamedTensor(
                    "group_id", TensorFrom(arrow::uint32(), "[0, 0, 1, 1, 2]")),
                .group_num = test::NamedTensor(
                    "group_num", TensorFrom(arrow::uint32(), "[3]")),
                .input = test::NamedTensor("in", TensorFrom(arrow::boolean(),
                                                            "[0, 0, 1, 1, 0]")),
                .output = test::NamedTensor("out", TensorFrom(arrow::int64(),
                                                              "[0, 2, 0]"))},
            GroupHeSumTestCase{
                .group_id = test::NamedTensor(
                    "group_id", TensorFrom(arrow::uint32(), "[0, 0, 1, 1, 2]")),
                .group_num = test::NamedTensor(
                    "group_num", TensorFrom(arrow::uint32(), "[3]")),
                .input = test::NamedTensor("in", TensorFrom(arrow::int64(),
                                                            "[0, 0, 1, 1, 2]")),
                .output = test::NamedTensor("out", TensorFrom(arrow::int64(),
                                                              "[0, 2, 2]"))},
            GroupHeSumTestCase{
                .group_id = test::NamedTensor(
                    "group_id", TensorFrom(arrow::uint32(), "[0, 1, 1, 0, 2]")),
                .group_num = test::NamedTensor(
                    "group_num", TensorFrom(arrow::uint32(), "[3]")),
                .input = test::NamedTensor(
                    "in",
                    TensorFrom(arrow::float64(), "[-1.1, 2.2, 3.3, 4.4, 5.6]")),
                .output = test::NamedTensor(
                    "out", TensorFrom(arrow::float64(), "[3.3, 5.5, 5.6]"))},
            GroupHeSumTestCase{
                .group_id = test::NamedTensor(
                    "group_id", TensorFrom(arrow::uint32(), "[]")),
                .group_num = test::NamedTensor(
                    "group_num", TensorFrom(arrow::uint32(), "[0]")),
                .input = test::NamedTensor("in",
                                           TensorFrom(arrow::float64(), "[]")),
                .output = test::NamedTensor("out", TensorFrom(arrow::float64(),
                                                              "[]"))})),
    TestParamNameGenerator(GroupHeSumTest));

}  // namespace scql::engine::op
