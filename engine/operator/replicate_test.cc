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

#include "engine/operator/replicate.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct ReplicateTestCase {
  std::vector<test::NamedTensor> left;
  std::vector<test::NamedTensor> right;
  std::vector<test::NamedTensor> expect_left;
  std::vector<test::NamedTensor> expect_right;
};

class ReplicateTest
    : public testing::TestWithParam<
          std::tuple<test::SpuRuntimeTestCase, ReplicateTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const ReplicateTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    ReplicateBatchTest, ReplicateTest,
    testing::Combine(
        testing::Values(test::SpuRuntimeTestCase{spu::ProtocolKind::SEMI2K, 2}),
        testing::Values(
            // test private status
            ReplicateTestCase{
                .left = {test::NamedTensor("left1",
                                           TensorFromJSON(arrow::int64(),
                                                          "[1, 2, null, 3]")),
                         test::NamedTensor(
                             "left2", TensorFromJSON(
                                          arrow::large_utf8(),
                                          R"json(["A", "B", "C", null])json"))},
                .right = {test::NamedTensor(
                    "right", TensorFromJSON(arrow::float64(), "[-1.1, null]"))},
                .expect_left =
                    {test::NamedTensor(
                         "el1",
                         TensorFromJSON(arrow::int64(),
                                        "[1, 2, null, 3, 1, 2, null, 3]")),
                     test::NamedTensor(
                         "el2",
                         TensorFromJSON(
                             arrow::large_utf8(),
                             R"json(["A", "B", "C", null, "A", "B", "C", null])json"))},
                .expect_right = {test::NamedTensor(
                    "er", TensorFromJSON(arrow::float64(),
                                         "[-1.1, -1.1, -1.1, -1.1, null, null, "
                                         "null, null]"))}})),
    TestParamNameGenerator(ReplicateTest));

TEST_P(ReplicateTest, works) {
  // Given
  auto parm = GetParam();
  auto tc = std::get<1>(parm);
  auto node = MakeExecNode(tc);
  auto sessions = test::MakeMultiPCSession(std::get<0>(parm));

  ExecContext alice_ctx(node, sessions[0].get());
  ExecContext bob_ctx(node, sessions[1].get());

  test::FeedInputsAsPrivate(&alice_ctx, tc.left);
  test::FeedInputsAsPrivate(&bob_ctx, tc.right);

  // When
  EXPECT_NO_THROW(test::RunAsync<Replicate>({&alice_ctx, &bob_ctx}));

  // Then
  // check alice output
  auto check_output = [&](ExecContext* ctx,
                          const std::vector<test::NamedTensor>& output) {
    for (size_t i = 0; i < output.size(); ++i) {
      auto t = ctx->GetTensorTable()->GetTensor(output[i].name);
      ASSERT_TRUE(t);
      auto out_arr = t->ToArrowChunkedArray();

      auto expect_arr = output[i].tensor->ToArrowChunkedArray();

      // compare tensor content
      EXPECT_TRUE(out_arr->Equals(expect_arr))
          << "expect type = " << expect_arr->type()->ToString()
          << ", got type = " << out_arr->type()->ToString()
          << "\nexpect result = " << expect_arr->ToString()
          << "\nbut actual got result = " << out_arr->ToString();
    }
  };

  check_output(&alice_ctx, tc.expect_left);
  check_output(&bob_ctx, tc.expect_right);
}

/// ===================
/// ReplicateTest impl
/// ===================

pb::ExecNode ReplicateTest::MakeExecNode(const ReplicateTestCase& tc) {
  test::ExecNodeBuilder builder(Replicate::kOpType);
  builder.SetNodeName("replicate-test");
  builder.AddStringsAttr(
      Replicate::kInputPartyCodesAttr,
      std::vector<std::string>{test::kPartyAlice, test::kPartyBob});

  std::vector<pb::Tensor> left;
  for (auto t : tc.left) {
    auto tmp = test::MakePrivateTensorReference(t.name, t.tensor->Type());
    left.push_back(tmp);
  }
  builder.AddInput(Replicate::kInLeft, left);

  std::vector<pb::Tensor> right;
  for (auto t : tc.right) {
    auto tmp = test::MakePrivateTensorReference(t.name, t.tensor->Type());
    right.push_back(tmp);
  }
  builder.AddInput(Replicate::kInRight, right);

  std::vector<pb::Tensor> expect_left;
  for (auto t : tc.expect_left) {
    auto tmp = test::MakePrivateTensorReference(t.name, t.tensor->Type());
    expect_left.push_back(tmp);
  }
  builder.AddOutput(Replicate::kOutLeft, expect_left);

  std::vector<pb::Tensor> expect_right;
  for (auto t : tc.expect_right) {
    auto tmp = test::MakePrivateTensorReference(t.name, t.tensor->Type());
    expect_right.push_back(tmp);
  }
  builder.AddOutput(Replicate::kOutRight, expect_right);

  return builder.Build();
}

}  // namespace scql::engine::op