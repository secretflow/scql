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

#include "engine/operator/plain_join.h"

#include <cstddef>
#include <cstdint>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"
#include "engine/util/psi_helper.h"

namespace scql::engine::op {

struct PlainJoinTestCase {
  std::vector<test::NamedTensor> left_inputs;
  std::vector<test::NamedTensor> right_inputs;
  // join indices in following format:
  // ["left_idx1,right_idx1", "left_idx2,right_idx2",...]
  std::vector<std::string> join_indices;
  int64_t ub_server;
};

class PlainJoinTest
    : public ::testing::TestWithParam<
          std::tuple<test::SpuRuntimeTestCase, PlainJoinTestCase>> {
 protected:
  static pb::ExecNode MakeJoinExecNode(const PlainJoinTestCase& tc);

  static void FeedInputs(ExecContext* ctx,
                         const std::vector<test::NamedTensor>& tensors);

  static constexpr char kOutLeft[] = "left_output";
  static constexpr char kOutRight[] = "right_output";
};

INSTANTIATE_TEST_SUITE_P(
    PlainJoinBatchTest, PlainJoinTest,
    testing::Combine(
        test::SpuTestValues2PC,
        testing::Values(
            // case 1
            PlainJoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "id_a", TensorFromJSON(arrow::int64(), "[4,8,1,3,3,5]"))},
                .right_inputs = {test::NamedTensor(
                    "id_b", TensorFromJSON(arrow::int64(), "[2,1,4,4,3,7,8]"))},
                .join_indices = {"0,2", "0,3", "1,6", "2,1", "3,4", "4,4"}},

            // case 2
            PlainJoinTestCase{
                .left_inputs =
                    {test::NamedTensor(
                         "x_1", TensorFromJSON(
                                    arrow::large_utf8(),
                                    R"json(["A","A","B","B","B","C"])json")),
                     test::NamedTensor("x_2", TensorFromJSON(arrow::int64(),
                                                             "[1,2,1,1,2,1]"))},
                .right_inputs =
                    {test::NamedTensor(
                         "y_1", TensorFromJSON(
                                    arrow::large_utf8(),
                                    R"json(["B","A","A","B","B","B"])json")),
                     test::NamedTensor("y_2", TensorFromJSON(arrow::int64(),
                                                             "[2,1,2,1,3,2]"))},
                .join_indices = {"0,1", "1,2", "2,3", "3,3", "4,0", "4,5"}},

            // case 3
            // testcase: empty join output
            PlainJoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFromJSON(arrow::large_utf8(),
                                        R"json(["A","B","C"])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFromJSON(arrow::large_utf8(),
                                        R"json(["E","F","H","G"])json"))},
                .join_indices = {}},

            // case 4
            // testcase: empty input
            PlainJoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFromJSON(arrow::large_utf8(), R"json([])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFromJSON(arrow::large_utf8(),
                                        R"json(["E","F","H","G"])json"))},
                .join_indices = {}})),

    TestParamNameGenerator(PlainJoinTest));

TEST_P(PlainJoinTest, works) {
  // Given
  auto parm = GetParam();
  auto test_case = std::get<1>(parm);
  pb::ExecNode node = MakeJoinExecNode(test_case);
  auto sessions = test::MakeMultiPCSession(std::get<0>(parm));

  ExecContext alice_ctx(node, sessions[0].get());

  // feed inputs
  FeedInputs(&alice_ctx, test_case.left_inputs);
  FeedInputs(&alice_ctx, test_case.right_inputs);

  // When
  EXPECT_NO_THROW(test::RunAsync<PlainJoin>({&alice_ctx}));

  // Then
  // left output
  auto left_output = alice_ctx.GetTensorTable()->GetTensor(kOutLeft);
  EXPECT_TRUE(left_output != nullptr);
  EXPECT_EQ(left_output->Type(), pb::PrimitiveDataType::INT64);

  // right output
  auto right_output = alice_ctx.GetTensorTable()->GetTensor(kOutRight);
  EXPECT_TRUE(right_output != nullptr);
  EXPECT_EQ(right_output->Type(), pb::PrimitiveDataType::INT64);

  EXPECT_EQ(left_output->Length(), right_output->Length());
  EXPECT_EQ(left_output->Length(), test_case.join_indices.size());

  auto left_join_result = util::StringifyVisitor(*left_output)
                              .StringifyBatch(left_output->Length());
  auto right_join_result = util::StringifyVisitor(*right_output)
                               .StringifyBatch(right_output->Length());

  auto indices_got = util::Combine(left_join_result, right_join_result);

  EXPECT_THAT(indices_got,
              ::testing::UnorderedElementsAreArray(test_case.join_indices));
}

/// ===========================
/// PlainJoinTest impl
/// ===========================

pb::ExecNode PlainJoinTest::MakeJoinExecNode(const PlainJoinTestCase& tc) {
  test::ExecNodeBuilder builder(PlainJoin::kOpType);

  builder.SetNodeName("plain-join-test");

  // Add inputs

  std::vector<pb::Tensor> left_inputs;
  for (const auto& named_tensor : tc.left_inputs) {
    auto t = test::MakeTensorReference(named_tensor.name,
                                       named_tensor.tensor->Type(),
                                       pb::TensorStatus::TENSORSTATUS_PRIVATE);
    left_inputs.push_back(std::move(t));
  }
  builder.AddInput(PlainJoin::kInLeft, left_inputs);

  std::vector<pb::Tensor> right_inputs;
  for (const auto& named_tensor : tc.right_inputs) {
    auto t = test::MakeTensorReference(named_tensor.name,
                                       named_tensor.tensor->Type(),
                                       pb::TensorStatus::TENSORSTATUS_PRIVATE);
    right_inputs.push_back(std::move(t));
  }
  builder.AddInput(PlainJoin::kInRight, right_inputs);

  // Add outputs
  auto left_join_output =
      test::MakeTensorReference(kOutLeft, pb::PrimitiveDataType::INT64,
                                pb::TensorStatus::TENSORSTATUS_PRIVATE);
  auto right_join_output =
      test::MakeTensorReference(kOutRight, pb::PrimitiveDataType::INT64,
                                pb::TensorStatus::TENSORSTATUS_PRIVATE);
  builder.AddOutput(PlainJoin::kOutLeftJoinIndex,
                    std::vector<pb::Tensor>{left_join_output});
  builder.AddOutput(PlainJoin::kOutRightJoinIndex,
                    std::vector<pb::Tensor>{right_join_output});

  return builder.Build();
}

void PlainJoinTest::FeedInputs(ExecContext* ctx,
                               const std::vector<test::NamedTensor>& tensors) {
  auto tensor_table = ctx->GetTensorTable();
  for (const auto& named_tensor : tensors) {
    tensor_table->AddTensor(named_tensor.name, named_tensor.tensor);
  }
}

}  // namespace scql::engine::op