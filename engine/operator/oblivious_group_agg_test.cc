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

#include "engine/operator/oblivious_group_agg.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_from_json.h"
#include "engine/operator/all_ops_register.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct ObliviousGroupAggTestCase {
  std::string op_type;
  std::vector<test::NamedTensor> inputs;
  test::NamedTensor group;
  std::vector<test::NamedTensor> outputs;
};

class ObliviousGroupAggTest
    : public testing::TestWithParam<
          std::tuple<spu::ProtocolKind, ObliviousGroupAggTestCase>> {
 protected:
  void SetUp() override { RegisterAllOps(); }

  static pb::ExecNode MakeExecNode(const ObliviousGroupAggTestCase& tc);
};

TEST_P(ObliviousGroupAggTest, works) {
  // Given
  auto parm = GetParam();
  auto tc = std::get<1>(parm);
  auto node = MakeExecNode(tc);
  std::vector<Session> sessions = test::Make2PCSession(std::get<0>(parm));

  ExecContext alice_ctx(node, &sessions[0]);
  ExecContext bob_ctx(node, &sessions[1]);

  // feed inputs
  test::FeedInputsAsSecret({&alice_ctx, &bob_ctx}, tc.inputs);
  test::FeedInputsAsSecret({&alice_ctx, &bob_ctx}, {tc.group});

  // When
  auto create_op = [](const std::string& op_type) {
    return GetOpRegistry()->GetOperator(op_type);
  };
  auto alice_op = create_op(node.op_type());
  auto bob_op = create_op(node.op_type());
  ASSERT_TRUE(alice_op != nullptr)
      << "failed to create operator for op_type = " << node.op_type();
  ASSERT_TRUE(bob_op != nullptr);

  test::OpAsyncRunner alice(alice_op.get());
  test::OpAsyncRunner bob(bob_op.get());

  alice.Start(&alice_ctx);
  bob.Start(&bob_ctx);

  // Then
  EXPECT_NO_THROW({ alice.Wait(); });
  EXPECT_NO_THROW({ bob.Wait(); });
  for (size_t i = 0; i < tc.inputs.size(); ++i) {
    TensorPtr actual_output = nullptr;
    EXPECT_NO_THROW({
      actual_output =
          test::RevealSecret({&alice_ctx, &bob_ctx}, tc.outputs[i].name);
    });
    ASSERT_TRUE(actual_output != nullptr);
    auto actual_arr = actual_output->ToArrowChunkedArray();
    auto expect_arr = tc.outputs[i].tensor->ToArrowChunkedArray();
    EXPECT_TRUE(actual_arr->ApproxEquals(
        *expect_arr, arrow::EqualOptions::Defaults().atol(0.01)))
        << "\nexpect result = " << expect_arr->ToString()
        << "\nbut actual got result = " << actual_arr->ToString();
  }
}

/// ===================
/// ObliviousGroupAggTest impl
/// ===================

pb::ExecNode ObliviousGroupAggTest::MakeExecNode(
    const ObliviousGroupAggTestCase& tc) {
  test::ExecNodeBuilder builder(tc.op_type);

  builder.SetNodeName("oblivious-group-agg-test");

  std::vector<pb::Tensor> inputs;
  for (const auto& named_tensor : tc.inputs) {
    pb::Tensor t = test::MakeSecretTensorReference(named_tensor.name,
                                                   named_tensor.tensor->Type());
    inputs.push_back(std::move(t));
  }
  builder.AddInput(ObliviousGroupAggBase::kIn, inputs);

  pb::Tensor t =
      test::MakeSecretTensorReference(tc.group.name, tc.group.tensor->Type());
  builder.AddInput(ObliviousGroupAggBase::kGroup, {t});

  std::vector<pb::Tensor> outputs;
  for (const auto& named_tensor : tc.outputs) {
    pb::Tensor t = test::MakeSecretTensorReference(named_tensor.name,
                                                   named_tensor.tensor->Type());
    outputs.push_back(std::move(t));
  }
  builder.AddOutput(ObliviousGroupAggBase::kOut, outputs);

  return builder.Build();
}

// =====================
// TEST_SUITE: ObliviousGroupSum
// =====================

INSTANTIATE_TEST_SUITE_P(
    ObliviousGroupSumTest, ObliviousGroupAggTest,
    testing::Combine(
        testing::Values(spu::ProtocolKind::CHEETAH, spu::ProtocolKind::SEMI2K),
        testing::Values(
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupSum::kOpType,
                .inputs = {test::NamedTensor(
                    "in",
                    TensorFromJSON(arrow::boolean(), "[true, false, true]"))},
                .group = test::NamedTensor(
                    "group", TensorFromJSON(arrow::boolean(), "[0, 0, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::int64(), "[1, 1, 2]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupSum::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::boolean(), "[true]"))},
                .group = test::NamedTensor(
                    "group", TensorFromJSON(arrow::boolean(), "[0]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::int64(), "[1]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupSum::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::int64(), "[1, 1, 1, 1, 1]"))},
                .group = test::NamedTensor("group",
                                           TensorFromJSON(arrow::boolean(),
                                                          "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::int64(), "[1, 1, 2, 3, 1]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupSum::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::float32(),
                                         "[-3.14, 1.1, 10, 100, 31415.9]"))},
                .group = test::NamedTensor("group",
                                           TensorFromJSON(arrow::boolean(),
                                                          "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out",
                    TensorFromJSON(arrow::float32(),
                                   "[-3.14, 1.1, 11.1, 111.1, 31415.9]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupSum::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::float32(), "[]"))},
                .group = test::NamedTensor(
                    "group", TensorFromJSON(arrow::boolean(), "[]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::float32(), "[]"))}})),
    TestParamNameGenerator(ObliviousGroupAggTest));

// =====================
// TEST_SUITE: ObliviousGroupCount
// =====================

INSTANTIATE_TEST_SUITE_P(
    ObliviousGroupCountTest, ObliviousGroupAggTest,
    testing::Combine(
        testing::Values(spu::ProtocolKind::CHEETAH, spu::ProtocolKind::SEMI2K),
        testing::Values(
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupCount::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::int64(), "[1, 2, 3, 4, 5]"))},
                .group = test::NamedTensor("group",
                                           TensorFromJSON(arrow::boolean(),
                                                          "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::int64(), "[1, 1, 2, 3, 1]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupCount::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::float32(),
                                         "[-3.14, 1.1, 10, 100, 31415.9]"))},
                .group = test::NamedTensor("group",
                                           TensorFromJSON(arrow::boolean(),
                                                          "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::int64(), "[1, 1, 2, 3, 1]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupCount::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::float32(), "[]"))},
                .group = test::NamedTensor(
                    "group", TensorFromJSON(arrow::boolean(), "[]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::int64(), "[]"))}})),
    TestParamNameGenerator(ObliviousGroupAggTest));

// =====================
// TEST_SUITE: ObliviousGroupAvg
// =====================

INSTANTIATE_TEST_SUITE_P(
    ObliviousGroupAvgTest, ObliviousGroupAggTest,
    testing::Combine(
        testing::Values(spu::ProtocolKind::CHEETAH, spu::ProtocolKind::SEMI2K),
        testing::Values(
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupAvg::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::int64(), "[1, 2, 3, 4, 5]"))},
                .group = test::NamedTensor("group",
                                           TensorFromJSON(arrow::boolean(),
                                                          "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out",
                    TensorFromJSON(arrow::float32(), "[1, 2, 2.5, 3, 5]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupAvg::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::float32(),
                                         "[-3.14, 1.3, 10, 100, 314.08]"))},
                .group = test::NamedTensor("group",
                                           TensorFromJSON(arrow::boolean(),
                                                          "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out",
                    TensorFromJSON(arrow::float32(),
                                   "[-3.14, 1.3, 5.65, 37.1, 314.08]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupAvg::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::float32(), "[]"))},
                .group = test::NamedTensor(
                    "group", TensorFromJSON(arrow::boolean(), "[]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::float32(), "[]"))}})),
    TestParamNameGenerator(ObliviousGroupAggTest));

// =====================
// TEST_SUITE: ObliviousGroupMax
// =====================

INSTANTIATE_TEST_SUITE_P(
    ObliviousGroupMaxTest, ObliviousGroupAggTest,
    testing::Combine(
        testing::Values(spu::ProtocolKind::CHEETAH, spu::ProtocolKind::SEMI2K),
        testing::Values(
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupMax::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::int64(), "[1, 2, 3, 4, 5]"))},
                .group = test::NamedTensor("group",
                                           TensorFromJSON(arrow::boolean(),
                                                          "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::int64(), "[1, 2, 3, 4, 5]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupMax::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::float32(),
                                         "[-3.14, 1.3, 10, 100, 314.08]"))},
                .group = test::NamedTensor("group",
                                           TensorFromJSON(arrow::boolean(),
                                                          "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::float32(),
                                          "[-3.14, 1.3, 10, 100, 314.08]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupMax::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::float32(), "[]"))},
                .group = test::NamedTensor(
                    "group", TensorFromJSON(arrow::boolean(), "[]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::float32(), "[]"))}})),
    TestParamNameGenerator(ObliviousGroupAggTest));

// =====================
// TEST_SUITE: ObliviousGroupMin
// =====================

INSTANTIATE_TEST_SUITE_P(
    ObliviousGroupMinTest, ObliviousGroupAggTest,
    testing::Combine(
        testing::Values(spu::ProtocolKind::CHEETAH, spu::ProtocolKind::SEMI2K),
        testing::Values(
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupMin::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::int64(), "[1, 2, 3, 4, 5]"))},
                .group = test::NamedTensor("group",
                                           TensorFromJSON(arrow::boolean(),
                                                          "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::int64(), "[1, 2, 2, 2, 5]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupMin::kOpType,
                .inputs = {test::NamedTensor("in_a",
                                             TensorFromJSON(arrow::int64(),
                                                            "[1, 2, 3, 4, 5]")),
                           test::NamedTensor(
                               "in_b", TensorFromJSON(
                                           arrow::float32(),
                                           "[-3.14, 1.3, 10, 100, 314.08]"))},
                .group = test::NamedTensor("group",
                                           TensorFromJSON(arrow::boolean(),
                                                          "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                                "out_a", TensorFromJSON(arrow::int64(),
                                                        "[1, 2, 2, 2, 5]")),
                            test::NamedTensor(
                                "out_b", TensorFromJSON(arrow::float32(),
                                                        "[-3.14, 1.3, 1.3, "
                                                        "1.3, 314.08]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupMin::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::float32(), "[]"))},
                .group = test::NamedTensor(
                    "group", TensorFromJSON(arrow::boolean(), "[]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::float32(), "[]"))}})),
    TestParamNameGenerator(ObliviousGroupAggTest));

}  // namespace scql::engine::op
