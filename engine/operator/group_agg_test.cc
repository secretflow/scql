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

#include "engine/operator/group_agg.h"

#include "arrow/type.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/all_ops_register.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct GroupAggTestCase {
  std::string op_type;
  std::vector<test::NamedTensor> inputs;
  test::NamedTensor group_id;
  test::NamedTensor group_num;
  std::vector<test::NamedTensor> outputs;
};

class GroupAggTest : public testing::TestWithParam<GroupAggTestCase> {
 protected:
  void SetUp() override { RegisterAllOps(); }

  static pb::ExecNode MakeExecNode(const GroupAggTestCase& tc);
};

TEST_P(GroupAggTest, works) {
  // Given
  auto tc = GetParam();
  auto node = MakeExecNode(tc);
  auto session = test::Make1PCSession();
  ExecContext ctx(node, session.get());

  test::FeedInputsAsPrivate(&ctx, tc.inputs);
  test::FeedInputsAsPrivate(&ctx, {tc.group_id, tc.group_num});

  // When
  auto op = GetOpRegistry()->GetOperator(node.op_type());
  ASSERT_TRUE(op);
  EXPECT_NO_THROW(op->Run(&ctx));

  // Then
  for (size_t i = 0; i < tc.inputs.size(); ++i) {
    auto actual_output = ctx.GetTensorTable()->GetTensor(tc.outputs[i].name);
    ASSERT_TRUE(actual_output);
    auto actual_arr = actual_output->ToArrowChunkedArray();
    auto expect_arr = tc.outputs[i].tensor->ToArrowChunkedArray();
    EXPECT_TRUE(actual_arr->ApproxEquals(
        *expect_arr, arrow::EqualOptions::Defaults().atol(0.01)))
        << "\nexpect result = " << expect_arr->ToString()
        << "\nbut actual got result = " << actual_arr->ToString();
  }
}

/// ===================
/// GroupAggTest impl
/// ===================

pb::ExecNode GroupAggTest::MakeExecNode(const GroupAggTestCase& tc) {
  test::ExecNodeBuilder builder(tc.op_type);

  builder.SetNodeName("plaintext-group-agg-test");

  std::vector<pb::Tensor> inputs;
  for (const auto& named_tensor : tc.inputs) {
    pb::Tensor t = test::MakePrivateTensorReference(
        named_tensor.name, named_tensor.tensor->Type());
    inputs.push_back(std::move(t));
  }
  builder.AddInput(GroupAggBase::kIn, inputs);

  pb::Tensor t_id = test::MakePrivateTensorReference(
      tc.group_id.name, tc.group_id.tensor->Type());
  builder.AddInput(GroupAggBase::kGroupId, {t_id});
  pb::Tensor t_num = test::MakePrivateTensorReference(
      tc.group_num.name, tc.group_num.tensor->Type());
  builder.AddInput(GroupAggBase::kGroupNum, {t_num});

  std::vector<pb::Tensor> outputs;
  for (const auto& named_tensor : tc.outputs) {
    pb::Tensor t = test::MakePrivateTensorReference(
        named_tensor.name, named_tensor.tensor->Type());
    outputs.push_back(std::move(t));
  }
  builder.AddOutput(GroupAggBase::kOut, outputs);

  return builder.Build();
}

INSTANTIATE_TEST_SUITE_P(
    GroupBatchTest, GroupAggTest,
    testing::Values(
        GroupAggTestCase{
            .op_type = GroupFirstOf::kOpType,
            .inputs =
                {test::NamedTensor("in_a", TensorFrom(arrow::int64(),
                                                      "[null, 0, 1, 1, 2]")),
                 test::NamedTensor("in_b", TensorFrom(arrow::float32(),
                                                      "[0, 0, 1.1, 1.1, 2.2]")),
                 test::NamedTensor(
                     "in_c", TensorFrom(arrow::large_utf8(),
                                        R"json(["A","A","B","B","CCC"])json"))},
            .group_id = test::NamedTensor(
                "group_id", TensorFrom(arrow::uint32(), "[0, 0, 1, 1, 2]")),
            .group_num = test::NamedTensor("group_num",
                                           TensorFrom(arrow::uint32(), "[3]")),
            .outputs = {test::NamedTensor("out_a", TensorFrom(arrow::int64(),
                                                              "[null, 1, 2]")),
                        test::NamedTensor("out_b", TensorFrom(arrow::float32(),
                                                              "[0, 1.1, 2.2]")),
                        test::NamedTensor(
                            "out_c",
                            TensorFrom(arrow::large_utf8(),
                                       R"json(["A","B","CCC"])json"))}},
        GroupAggTestCase{
            .op_type = GroupCountDistinct::kOpType,
            .inputs = {test::NamedTensor("in_a",
                                         TensorFrom(arrow::int64(),
                                                    "[null, 0, 1, 1, 2]")),
                       test::NamedTensor("in_b",
                                         TensorFrom(arrow::float32(),
                                                    "[0, 0, 1.1, 1.1, 2.2]"))},
            .group_id = test::NamedTensor(
                "group_id", TensorFrom(arrow::uint32(), "[0, 0, 1, 1, 2]")),
            .group_num = test::NamedTensor("group_num",
                                           TensorFrom(arrow::uint32(), "[3]")),
            .outputs = {test::NamedTensor("out_a", TensorFrom(arrow::int64(),
                                                              "[1, 1, 1]")),
                        test::NamedTensor("out_b", TensorFrom(arrow::int64(),
                                                              "[1, 1, 1]"))}},
        GroupAggTestCase{
            .op_type = GroupCount::kOpType,
            .inputs = {test::NamedTensor("in_a",
                                         TensorFrom(arrow::int64(),
                                                    "[null, 0, 1, 1, 2]")),
                       test::NamedTensor("in_b",
                                         TensorFrom(arrow::float32(),
                                                    "[0, 0, 1.1, 1.1, 2.2]"))},
            .group_id = test::NamedTensor(
                "group_id", TensorFrom(arrow::uint32(), "[0, 0, 1, 1, 2]")),
            .group_num = test::NamedTensor("group_num",
                                           TensorFrom(arrow::uint32(), "[3]")),
            .outputs = {test::NamedTensor("out_a", TensorFrom(arrow::int64(),
                                                              "[1, 2, 1]")),
                        test::NamedTensor("out_b", TensorFrom(arrow::int64(),
                                                              "[2, 2, 1]"))}},
        GroupAggTestCase{
            .op_type = GroupSum::kOpType,
            .inputs =
                {test::NamedTensor("in_a", TensorFrom(arrow::int64(),
                                                      "[0, 1, 2, 3, null]")),
                 test::NamedTensor(
                     "in_b",
                     TensorFrom(arrow::float32(), "[0, 1.1, 2.2, 3.3, 4.4]"))},
            .group_id = test::NamedTensor(
                "group_id", TensorFrom(arrow::uint32(), "[0, 0, 1, 1, 2]")),
            .group_num = test::NamedTensor("group_num",
                                           TensorFrom(arrow::uint32(), "[3]")),
            .outputs = {test::NamedTensor("out_a", TensorFrom(arrow::int64(),
                                                              "[1, 5, null]")),
                        test::NamedTensor("out_b",
                                          TensorFrom(arrow::float64(),
                                                     "[1.1, 5.5, 4.4]"))}},
        GroupAggTestCase{
            .op_type = GroupAvg::kOpType,
            .inputs =
                {test::NamedTensor("in_a", TensorFrom(arrow::int64(),
                                                      "[0, 1, 2, 3, null]")),
                 test::NamedTensor(
                     "in_b",
                     TensorFrom(arrow::float32(), "[0, 1.1, 2.2, 3.3, 4.4]"))},
            .group_id = test::NamedTensor(
                "group_id", TensorFrom(arrow::uint32(), "[0, 0, 1, 1, 2]")),
            .group_num = test::NamedTensor("group_num",
                                           TensorFrom(arrow::uint32(), "[3]")),
            .outputs = {test::NamedTensor("out_a",
                                          TensorFrom(arrow::float64(),
                                                     "[0.5, 2.5, null]")),
                        test::NamedTensor("out_b",
                                          TensorFrom(arrow::float64(),
                                                     "[0.55, 2.75, 4.4]"))}},
        GroupAggTestCase{
            .op_type = GroupMin::kOpType,
            .inputs =
                {test::NamedTensor("in_a", TensorFrom(arrow::int64(),
                                                      "[0, 1, 2, 3, null]")),
                 test::NamedTensor(
                     "in_b",
                     TensorFrom(arrow::float32(), "[0, 1.1, 2.2, 3.3, 4.4]"))},
            .group_id = test::NamedTensor(
                "group_id", TensorFrom(arrow::uint32(), "[0, 0, 1, 1, 2]")),
            .group_num = test::NamedTensor("group_num",
                                           TensorFrom(arrow::uint32(), "[3]")),
            .outputs = {test::NamedTensor("out_a", TensorFrom(arrow::int64(),
                                                              "[0, 2, null]")),
                        test::NamedTensor("out_b",
                                          TensorFrom(arrow::float32(),
                                                     "[0, 2.2, 4.4]"))}},
        GroupAggTestCase{
            .op_type = GroupMax::kOpType,
            .inputs =
                {test::NamedTensor("in_a", TensorFrom(arrow::int64(),
                                                      "[0, 1, 2, 3, null]")),
                 test::NamedTensor(
                     "in_b",
                     TensorFrom(arrow::float32(), "[0, 1.1, 2.2, 3.3, 4.4]"))},
            .group_id = test::NamedTensor(
                "group_id", TensorFrom(arrow::uint32(), "[0, 0, 1, 1, 2]")),
            .group_num = test::NamedTensor("group_num",
                                           TensorFrom(arrow::uint32(), "[3]")),
            .outputs = {
                test::NamedTensor("out_a",
                                  TensorFrom(arrow::int64(), "[1, 3, null]")),
                test::NamedTensor("out_b", TensorFrom(arrow::float32(),
                                                      "[1.1, 3.3, 4.4]"))}}));

INSTANTIATE_TEST_SUITE_P(
    GroupBatchEmptyTest, GroupAggTest,
    testing::Values(
        GroupAggTestCase{
            .op_type = GroupFirstOf::kOpType,
            .inputs =
                {test::NamedTensor("in_a", TensorFrom(arrow::int64(), "[]")),
                 test::NamedTensor("in_b", TensorFrom(arrow::float32(), "[]")),
                 test::NamedTensor("in_c", TensorFrom(arrow::large_utf8(),
                                                      R"json([])json"))},
            .group_id = test::NamedTensor("group_id",
                                          TensorFrom(arrow::uint32(), "[]")),
            .group_num = test::NamedTensor("group_num",
                                           TensorFrom(arrow::uint32(), "[0]")),
            .outputs =
                {test::NamedTensor("out_a", TensorFrom(arrow::int64(), "[]")),
                 test::NamedTensor("out_b", TensorFrom(arrow::float32(), "[]")),
                 test::NamedTensor("out_c", TensorFrom(arrow::large_utf8(),
                                                       R"json([])json"))}},
        GroupAggTestCase{
            .op_type = GroupCountDistinct::kOpType,
            .inputs =
                {test::NamedTensor("in_a", TensorFrom(arrow::int64(), "[]")),
                 test::NamedTensor("in_b", TensorFrom(arrow::float32(), "[]"))},
            .group_id = test::NamedTensor("group_id",
                                          TensorFrom(arrow::uint32(), "[]")),
            .group_num = test::NamedTensor("group_num",
                                           TensorFrom(arrow::uint32(), "[0]")),
            .outputs =
                {test::NamedTensor("out_a", TensorFrom(arrow::int64(), "[]")),
                 test::NamedTensor("out_b", TensorFrom(arrow::int64(), "[]"))}},
        GroupAggTestCase{
            .op_type = GroupCount::kOpType,
            .inputs =
                {test::NamedTensor("in_a", TensorFrom(arrow::int64(), "[]")),
                 test::NamedTensor("in_b", TensorFrom(arrow::float32(), "[]"))},
            .group_id = test::NamedTensor("group_id",
                                          TensorFrom(arrow::uint32(), "[]")),
            .group_num = test::NamedTensor("group_num",
                                           TensorFrom(arrow::uint32(), "[0]")),
            .outputs =
                {test::NamedTensor("out_a", TensorFrom(arrow::int64(), "[]")),
                 test::NamedTensor("out_b", TensorFrom(arrow::int64(), "[]"))}},
        GroupAggTestCase{
            .op_type = GroupSum::kOpType,
            .inputs =
                {test::NamedTensor("in_a", TensorFrom(arrow::int64(), "[]")),
                 test::NamedTensor("in_b", TensorFrom(arrow::float32(), "[]"))},
            .group_id = test::NamedTensor("group_id",
                                          TensorFrom(arrow::uint32(), "[]")),
            .group_num = test::NamedTensor("group_num",
                                           TensorFrom(arrow::uint32(), "[0]")),
            .outputs = {test::NamedTensor("out_a",
                                          TensorFrom(arrow::int64(), "[]")),
                        test::NamedTensor("out_b",
                                          TensorFrom(arrow::float64(), "[]"))}},
        GroupAggTestCase{
            .op_type = GroupAvg::kOpType,
            .inputs =
                {test::NamedTensor("in_a", TensorFrom(arrow::int64(), "[]")),
                 test::NamedTensor("in_b", TensorFrom(arrow::float32(), "[]"))},
            .group_id = test::NamedTensor("group_id",
                                          TensorFrom(arrow::uint32(), "[]")),
            .group_num = test::NamedTensor("group_num",
                                           TensorFrom(arrow::uint32(), "[0]")),
            .outputs = {test::NamedTensor("out_a",
                                          TensorFrom(arrow::float64(), "[]")),
                        test::NamedTensor("out_b",
                                          TensorFrom(arrow::float64(), "[]"))}},
        GroupAggTestCase{
            .op_type = GroupMin::kOpType,
            .inputs =
                {test::NamedTensor("in_a", TensorFrom(arrow::int64(), "[]")),
                 test::NamedTensor("in_b", TensorFrom(arrow::float32(), "[]"))},
            .group_id = test::NamedTensor("group_id",
                                          TensorFrom(arrow::uint32(), "[]")),
            .group_num = test::NamedTensor("group_num",
                                           TensorFrom(arrow::uint32(), "[0]")),
            .outputs = {test::NamedTensor("out_a",
                                          TensorFrom(arrow::int64(), "[]")),
                        test::NamedTensor("out_b",
                                          TensorFrom(arrow::float32(), "[]"))}},
        GroupAggTestCase{
            .op_type = GroupMax::kOpType,
            .inputs =
                {test::NamedTensor("in_a", TensorFrom(arrow::int64(), "[]")),
                 test::NamedTensor("in_b", TensorFrom(arrow::float32(), "[]"))},
            .group_id = test::NamedTensor("group_id",
                                          TensorFrom(arrow::uint32(), "[]")),
            .group_num = test::NamedTensor("group_num",
                                           TensorFrom(arrow::uint32(), "[0]")),
            .outputs = {
                test::NamedTensor("out_a", TensorFrom(arrow::int64(), "[]")),
                test::NamedTensor("out_b",
                                  TensorFrom(arrow::float32(), "[]"))}}));

}  // namespace scql::engine::op
