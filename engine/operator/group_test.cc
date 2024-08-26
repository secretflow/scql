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

#include "engine/operator/group.h"

#include "arrow/type.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct GroupTestCase {
  std::vector<test::NamedTensor> inputs;
  test::NamedTensor group_id;
  test::NamedTensor group_num;
};

class GroupTest : public testing::TestWithParam<GroupTestCase> {
 protected:
  static pb::ExecNode MakeExecNode(const GroupTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    GroupBatchTest, GroupTest,
    testing::Values(
        GroupTestCase{
            .inputs = {test::NamedTensor(
                "in",
                TensorFrom(arrow::int64(), "[2, 1, 1, 1, null, 0, null]"))},
            .group_id = test::NamedTensor(
                "out_id", TensorFrom(arrow::uint32(), "[0, 1, 1, 1, 2, 3, 2]")),
            .group_num = test::NamedTensor("out_num",
                                           TensorFrom(arrow::uint32(), "[4]"))},
        GroupTestCase{
            .inputs =
                {test::NamedTensor("in_a", TensorFrom(arrow::int64(),
                                                      "[0, 0, 1, 1, 1, 1]")),
                 test::NamedTensor("in_b",
                                   TensorFrom(arrow::float32(),
                                              "[-1, 0, 0, 3.14, 3.14, 3.14]")),
                 test::NamedTensor(
                     "in_c",
                     TensorFrom(arrow::large_utf8(),
                                R"json(["A","B","B","CCC","CCC","CCC"])json"))},
            .group_id = test::NamedTensor(
                "out_id", TensorFrom(arrow::uint32(), "[0, 1, 2, 3, 3, 3]")),
            .group_num = test::NamedTensor("out_num",
                                           TensorFrom(arrow::uint32(), "[4]"))},
        GroupTestCase{
            .inputs = {test::NamedTensor("in",
                                         TensorFrom(arrow::int64(), "[1]"))},
            .group_id = test::NamedTensor("out_id",
                                          TensorFrom(arrow::uint32(), "[0]")),
            .group_num = test::NamedTensor("out_num",
                                           TensorFrom(arrow::uint32(), "[1]"))},
        GroupTestCase{
            .inputs = {test::NamedTensor("in",
                                         TensorFrom(arrow::int64(), "[]"))},
            .group_id = test::NamedTensor("out_id",
                                          TensorFrom(arrow::uint32(), "[]")),
            .group_num = test::NamedTensor(
                "out_num", TensorFrom(arrow::uint32(), "[0]"))}));

TEST_P(GroupTest, works) {
  // Given
  auto tc = GetParam();
  auto node = MakeExecNode(tc);
  auto session = test::Make1PCSession();
  ExecContext ctx(node, session.get());

  test::FeedInputsAsPrivate(&ctx, tc.inputs);

  // When
  Group op;
  EXPECT_NO_THROW(op.Run(&ctx));

  // Then
  for (const auto& check_tensor :
       std::vector<test::NamedTensor>{tc.group_id, tc.group_num}) {
    auto expect_arr = check_tensor.tensor->ToArrowChunkedArray();
    auto out = ctx.GetTensorTable()->GetTensor(check_tensor.name);
    ASSERT_TRUE(out);
    auto out_arr = out->ToArrowChunkedArray();
    // compare tensor content
    EXPECT_TRUE(out_arr->ApproxEquals(*expect_arr))
        << "expect type = " << expect_arr->type()->ToString()
        << ", got type = " << out_arr->type()->ToString()
        << "\nexpect result = " << expect_arr->ToString()
        << "\nbut actual got result = " << out_arr->ToString();
  }
}

/// ===================
/// GroupTest impl
/// ===================

pb::ExecNode GroupTest::MakeExecNode(const GroupTestCase& tc) {
  test::ExecNodeBuilder builder(Group::kOpType);

  builder.SetNodeName("plaintext-group-test");

  std::vector<pb::Tensor> inputs;
  for (const auto& named_tensor : tc.inputs) {
    pb::Tensor t = test::MakePrivateTensorReference(
        named_tensor.name, named_tensor.tensor->Type());
    inputs.push_back(std::move(t));
  }
  builder.AddInput(Group::kIn, inputs);

  pb::Tensor group_id = test::MakePrivateTensorReference(
      tc.group_id.name, tc.group_id.tensor->Type());
  builder.AddOutput(Group::kOutId, {group_id});
  pb::Tensor group_num = test::MakePrivateTensorReference(
      tc.group_num.name, tc.group_num.tensor->Type());
  builder.AddOutput(Group::kOutNum, {group_num});

  return builder.Build();
}

}  // namespace scql::engine::op