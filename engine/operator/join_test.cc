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

#include "engine/operator/join.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_from_json.h"
#include "engine/operator/test_util.h"
#include "engine/util/psi_helper.h"

namespace scql::engine::op {

struct JoinTestCase {
  std::vector<test::NamedTensor> left_inputs;
  std::vector<test::NamedTensor> right_inputs;

  // join indices in following format:
  // ["left_idx1,right_idx1", "left_idx2,right_idx2",...]
  std::vector<std::string> join_indices;
};

class JoinTest : public ::testing::TestWithParam<
                     std::tuple<spu::ProtocolKind, JoinTestCase>> {
 protected:
  static pb::ExecNode MakeJoinExecNode(const JoinTestCase& tc);

  static void FeedInputs(ExecContext* ctx,
                         const std::vector<test::NamedTensor>& tensors);

  static constexpr char kOutLeft[] = "left_output";
  static constexpr char kOutRight[] = "right_output";
};

INSTANTIATE_TEST_SUITE_P(
    JoinBatchTest, JoinTest,
    testing::Combine(
        testing::Values(spu::ProtocolKind::CHEETAH, spu::ProtocolKind::SEMI2K),
        testing::Values(
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "id_a", TensorFromJSON(arrow::int64(), "[4,8,1,3,3,5]"))},
                .right_inputs = {test::NamedTensor(
                    "id_b", TensorFromJSON(arrow::int64(), "[2,1,4,4,3,7,8]"))},
                .join_indices = {"0,2", "0,3", "1,6", "2,1", "3,4", "4,4"}},
            // clang-format off
        // Given 
        //   input table ta & tb
        //        ta                   tb
        //    index, x_1, x_2      index, y_1, y_2
        //        0,   A, 1            0,   B, 2
        //        1,   A, 2            1,   A, 1 
        //        2,   B, 1            2,   A, 2
        //        3,   B, 1            3,   B, 1
        //        4,   B, 2            4,   B, 3
        //        5,   C, 1            5,   B, 2
        // 
        // Let 
        //   ta inner join tb on x_1=y_1 and x_2 = y_2
        // Then
        //   join table is:
        //         ta_index, x_1, x_2,    tb_index, y_1, y_2
        //                0,   A,   1,           1,   A, 1
        //                1,   A,   2,           2,   A, 2
        //                2,   B,   1,           3,   B, 1
        //                3,   B,   1,           3,   B, 1
        //                4,   B,   2,           0,   B, 2
        //                4,   B,   2,           5,   B, 2
            // clang-format on
            JoinTestCase{
                .left_inputs =
                    {test::NamedTensor(
                         "x_1", TensorFromJSON(
                                    arrow::utf8(),
                                    R"json(["A","A","B","B","B","C"])json")),
                     test::NamedTensor("x_2", TensorFromJSON(arrow::int64(),
                                                             "[1,2,1,1,2,1]"))},
                .right_inputs =
                    {test::NamedTensor(
                         "y_1", TensorFromJSON(
                                    arrow::utf8(),
                                    R"json(["B","A","A","B","B","B"])json")),
                     test::NamedTensor("y_2", TensorFromJSON(arrow::int64(),
                                                             "[2,1,2,1,3,2]"))},
                .join_indices = {"0,1", "1,2", "2,3", "3,3", "4,0", "4,5"}},
            // testcase: empty join output
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x",
                    TensorFromJSON(arrow::utf8(), R"json(["A","B","C"])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFromJSON(arrow::utf8(),
                                        R"json(["E","F","H","G"])json"))},
                .join_indices = {}},
            // testcase: empty input
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFromJSON(arrow::utf8(), R"json([])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFromJSON(arrow::utf8(),
                                        R"json(["E","F","H","G"])json"))},
                .join_indices = {}})),
    TestParamNameGenerator(JoinTest));

TEST_P(JoinTest, works) {
  // Given
  auto parm = GetParam();
  auto test_case = std::get<1>(parm);
  pb::ExecNode node = MakeJoinExecNode(test_case);
  std::vector<Session> sessions = test::Make2PCSession(std::get<0>(parm));

  ExecContext alice_ctx(node, &sessions[0]);
  ExecContext bob_ctx(node, &sessions[1]);

  // feed inputs
  FeedInputs(&alice_ctx, test_case.left_inputs);
  FeedInputs(&bob_ctx, test_case.right_inputs);

  // When
  test::OperatorTestRunner<Join> alice;
  test::OperatorTestRunner<Join> bob;

  alice.Start(&alice_ctx);
  bob.Start(&bob_ctx);

  // Then
  EXPECT_NO_THROW({ alice.Wait(); });
  EXPECT_NO_THROW({ bob.Wait(); });

  // left output
  auto left_output = alice_ctx.GetTensorTable()->GetTensor(kOutLeft);
  EXPECT_TRUE(left_output != nullptr);
  EXPECT_EQ(left_output->Type(), pb::PrimitiveDataType::INT64);

  // right output
  auto right_output = bob_ctx.GetTensorTable()->GetTensor(kOutRight);
  EXPECT_TRUE(right_output != nullptr);
  EXPECT_EQ(right_output->Type(), pb::PrimitiveDataType::INT64);

  EXPECT_EQ(left_output->Length(), right_output->Length());
  EXPECT_EQ(left_output->Length(), test_case.join_indices.size());

  util::BatchProvider provider(
      std::vector<TensorPtr>{left_output, right_output});

  auto indices_got = provider.ReadNextBatch(right_output->Length());
  EXPECT_THAT(indices_got,
              ::testing::UnorderedElementsAreArray(test_case.join_indices));
}

/// ===========================
/// JoinTest impl
/// ===========================

pb::ExecNode JoinTest::MakeJoinExecNode(const JoinTestCase& tc) {
  test::ExecNodeBuilder builder(Join::kOpType);

  builder.SetNodeName("join-test");
  builder.AddInt64Attr(Join::kJoinTypeAttr, Join::kInnerJoin);
  builder.AddStringsAttr(
      Join::kInputPartyCodesAttr,
      std::vector<std::string>{test::kPartyAlice, test::kPartyBob});

  // Add inputs

  std::vector<pb::Tensor> left_inputs;
  for (const auto& named_tensor : tc.left_inputs) {
    auto t = test::MakeTensorReference(named_tensor.name,
                                       named_tensor.tensor->Type(),
                                       pb::TensorStatus::TENSORSTATUS_PRIVATE);
    left_inputs.push_back(std::move(t));
  }
  builder.AddInput(Join::kInLeft, left_inputs);

  std::vector<pb::Tensor> right_inputs;
  for (const auto& named_tensor : tc.right_inputs) {
    auto t = test::MakeTensorReference(named_tensor.name,
                                       named_tensor.tensor->Type(),
                                       pb::TensorStatus::TENSORSTATUS_PRIVATE);
    right_inputs.push_back(std::move(t));
  }
  builder.AddInput(Join::kInRight, right_inputs);

  // Add outputs
  auto left_join_output =
      test::MakeTensorReference(kOutLeft, pb::PrimitiveDataType::INT64,
                                pb::TensorStatus::TENSORSTATUS_PRIVATE);
  auto right_join_output =
      test::MakeTensorReference(kOutRight, pb::PrimitiveDataType::INT64,
                                pb::TensorStatus::TENSORSTATUS_PRIVATE);
  builder.AddOutput(Join::kOutLeftJoinIndex,
                    std::vector<pb::Tensor>{left_join_output});
  builder.AddOutput(Join::kOutRightJoinIndex,
                    std::vector<pb::Tensor>{right_join_output});

  return builder.Build();
}

void JoinTest::FeedInputs(ExecContext* ctx,
                          const std::vector<test::NamedTensor>& tensors) {
  auto tensor_table = ctx->GetTensorTable();
  for (const auto& named_tensor : tensors) {
    tensor_table->AddTensor(named_tensor.name, named_tensor.tensor);
  }
}

}  // namespace scql::engine::op