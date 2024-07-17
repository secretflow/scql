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

#include "engine/operator/vertical_group_agg.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/all_ops_register.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct VerticalGroupAggTestCase {
  std::string op_type;
  std::vector<test::NamedTensor> inputs;
  test::NamedTensor group;
  std::vector<test::NamedTensor> outputs;
};

class VerticalGroupAggTest
    : public testing::TestWithParam<
          std::tuple<test::SpuRuntimeTestCase, VerticalGroupAggTestCase>> {
 protected:
  void SetUp() override { RegisterAllOps(); }

  static pb::ExecNode MakeExecNode(const VerticalGroupAggTestCase& tc);
};

TEST_P(VerticalGroupAggTest, works) {
  // Given
  auto parm = GetParam();
  auto tc = std::get<1>(parm);
  auto node = MakeExecNode(tc);
  auto sessions = test::MakeMultiPCSession(std::get<0>(parm));

  std::vector<ExecContext> exec_ctxs;
  for (size_t idx = 0; idx < sessions.size(); ++idx) {
    exec_ctxs.emplace_back(node, sessions[idx].get());
  }

  // feed inputs
  std::vector<ExecContext*> ctx_ptrs;
  for (size_t idx = 0; idx < exec_ctxs.size(); ++idx) {
    ctx_ptrs.emplace_back(&exec_ctxs[idx]);
  }
  test::FeedInputsAsSecret(ctx_ptrs, tc.inputs);
  test::FeedInputsAsPublic(ctx_ptrs, {tc.group});

  // When
  EXPECT_NO_THROW(test::RunOpAsync(ctx_ptrs, [&]() {
    return GetOpRegistry()->GetOperator(node.op_type());
  }));

  for (size_t i = 0; i < tc.inputs.size(); ++i) {
    TensorPtr actual_output = nullptr;
    EXPECT_NO_THROW(
        { actual_output = test::RevealSecret(ctx_ptrs, tc.outputs[i].name); });
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
/// VerticalGroupAggTest impl
/// ===================

pb::ExecNode VerticalGroupAggTest::MakeExecNode(
    const VerticalGroupAggTestCase& tc) {
  test::ExecNodeBuilder builder(tc.op_type);

  builder.SetNodeName("vertical-group-agg-test");

  std::vector<pb::Tensor> inputs;
  for (const auto& named_tensor : tc.inputs) {
    pb::Tensor t = test::MakeSecretTensorReference(named_tensor.name,
                                                   named_tensor.tensor->Type());
    inputs.push_back(std::move(t));
  }
  builder.AddInput(VerticalGroupAggBase::kIn, inputs);

  pb::Tensor t =
      test::MakePublicTensorReference(tc.group.name, tc.group.tensor->Type());
  builder.AddInput(VerticalGroupAggBase::kGroup, {t});

  std::vector<pb::Tensor> outputs;
  for (const auto& named_tensor : tc.outputs) {
    pb::Tensor t = test::MakeSecretTensorReference(named_tensor.name,
                                                   named_tensor.tensor->Type());
    outputs.push_back(std::move(t));
  }
  builder.AddOutput(VerticalGroupAggBase::kOut, outputs);

  return builder.Build();
}

// =====================
// TEST_SUITE: VerticalGroupSum
// =====================

INSTANTIATE_TEST_SUITE_P(
    VerticalGroupSumTest, VerticalGroupAggTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            VerticalGroupAggTestCase{
                .op_type = VerticalGroupSum::kOpType,
                .inputs = {test::NamedTensor(
                    "in",
                    TensorFromJSON(arrow::boolean(), "[true, false, true]"))},
                .group = test::NamedTensor(
                    "group", TensorFromJSON(arrow::boolean(), "[0, 0, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::int64(), "[2]"))}},
            VerticalGroupAggTestCase{
                .op_type = VerticalGroupSum::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::boolean(), "[true]"))},
                .group = test::NamedTensor(
                    "group", TensorFromJSON(arrow::boolean(), "[1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::int64(), "[1]"))}},
            VerticalGroupAggTestCase{
                .op_type = VerticalGroupSum::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::int64(), "[1, 1, 1, 1, 1]"))},
                .group = test::NamedTensor("group",
                                           TensorFromJSON(arrow::boolean(),
                                                          "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::int64(), "[1, 3, 1]"))}},
            VerticalGroupAggTestCase{
                .op_type = VerticalGroupSum::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::float32(),
                                         "[-3.14, 1.1, 10, 100, 31415.9]"))},
                .group = test::NamedTensor("group",
                                           TensorFromJSON(arrow::boolean(),
                                                          "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::float32(),
                                          "[-3.14, 111.1, 31415.9]"))}},
            VerticalGroupAggTestCase{
                .op_type = VerticalGroupSum::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::float32(), "[]"))},
                .group = test::NamedTensor(
                    "group", TensorFromJSON(arrow::boolean(), "[]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::float32(), "[]"))}})),
    TestParamNameGenerator(VerticalGroupAggTest));

// =====================
// TEST_SUITE: VerticalGroupCount
// =====================

INSTANTIATE_TEST_SUITE_P(
    VerticalGroupCountTest, VerticalGroupAggTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            VerticalGroupAggTestCase{
                .op_type = VerticalGroupCount::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::int64(), "[1, 2, 3, 4, 5]"))},
                .group = test::NamedTensor("group",
                                           TensorFromJSON(arrow::boolean(),
                                                          "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::int64(), "[1, 3, 1]"))}},
            VerticalGroupAggTestCase{
                .op_type = VerticalGroupCount::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::float32(),
                                         "[-3.14, 1.1, 10,100, 31415.9]"))},
                .group = test::NamedTensor("group",
                                           TensorFromJSON(arrow::boolean(),
                                                          "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::int64(), "[1, 3, 1]"))}},
            VerticalGroupAggTestCase{
                .op_type = VerticalGroupCount::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::float32(), "[]"))},
                .group = test::NamedTensor(
                    "group", TensorFromJSON(arrow::boolean(), "[]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::int64(), "[]"))}})),
    TestParamNameGenerator(VerticalGroupAggTest));

// =====================
// TEST_SUITE: VerticalGroupAvg
// =====================

INSTANTIATE_TEST_SUITE_P(
    VerticalGroupAvgTest, VerticalGroupAggTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            VerticalGroupAggTestCase{
                .op_type = VerticalGroupAvg::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::int64(), "[1, 2, 3, 4, 5]"))},
                .group = test::NamedTensor("group",
                                           TensorFromJSON(arrow::boolean(),
                                                          "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::float64(), "[1, 3, 5]"))}},
            VerticalGroupAggTestCase{
                .op_type = VerticalGroupAvg::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::float32(),
                                         "[-3.14, 1.3, 10, 100, 314.08]"))},
                .group = test::NamedTensor("group",
                                           TensorFromJSON(arrow::boolean(),
                                                          "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::float64(),
                                          "[-3.14, 37.1, 314.08]"))}},
            VerticalGroupAggTestCase{
                .op_type = VerticalGroupAvg::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::float32(), "[]"))},
                .group = test::NamedTensor(
                    "group", TensorFromJSON(arrow::boolean(), "[]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::float64(), "[]"))}})),
    TestParamNameGenerator(VerticalGroupAggTest));

// =====================
// TEST_SUITE: VerticalGroupMax
// =====================

INSTANTIATE_TEST_SUITE_P(
    VerticalGroupMaxTest, VerticalGroupAggTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            VerticalGroupAggTestCase{
                .op_type = VerticalGroupMax::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::int64(), "[1, 2, 3, 4, 5]"))},
                .group = test::NamedTensor("group",
                                           TensorFromJSON(arrow::boolean(),
                                                          "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::int64(), "[1, 4, 5]"))}},
            VerticalGroupAggTestCase{
                .op_type = VerticalGroupMax::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::float32(),
                                         "[-3.14, 1.3, 10, 100, 314.08]"))},
                .group = test::NamedTensor("group",
                                           TensorFromJSON(arrow::boolean(),
                                                          "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out",
                    TensorFromJSON(arrow::float32(), "[-3.14, 100, 314.08]"))}},
            VerticalGroupAggTestCase{
                .op_type = VerticalGroupMax::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::float32(), "[]"))},
                .group = test::NamedTensor(
                    "group", TensorFromJSON(arrow::boolean(), "[]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::float32(), "[]"))}})),
    TestParamNameGenerator(VerticalGroupAggTest));

// =====================
// TEST_SUITE: VerticalGroupMin
// =====================

INSTANTIATE_TEST_SUITE_P(
    VerticalGroupMinTest, VerticalGroupAggTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            VerticalGroupAggTestCase{
                .op_type = VerticalGroupMin::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::int64(), "[1, 2, 3, 4, 5]"))},
                .group = test::NamedTensor("group",
                                           TensorFromJSON(arrow::boolean(),
                                                          "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::int64(), "[1, 2, 5]"))}},
            VerticalGroupAggTestCase{
                .op_type = VerticalGroupMin::kOpType,
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
                .outputs =
                    {test::NamedTensor("out_a", TensorFromJSON(arrow::int64(),
                                                               "[1, 2, 5]")),
                     test::NamedTensor(
                         "out_b", TensorFromJSON(arrow::float32(),
                                                 "[-3.14, 1.3, 314.08]"))}},
            VerticalGroupAggTestCase{
                .op_type = VerticalGroupMin::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFromJSON(arrow::float32(), "[]"))},
                .group = test::NamedTensor(
                    "group", TensorFromJSON(arrow::boolean(), "[]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFromJSON(arrow::float32(), "[]"))}})),
    TestParamNameGenerator(VerticalGroupAggTest));

}  // namespace scql::engine::op
