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

#include "engine/core/tensor_constructor.h"
#include "engine/operator/all_ops_register.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct ObliviousGroupAggTestCase {
  std::string op_type;
  std::vector<test::NamedTensor> inputs;
  test::NamedTensor group;
  std::vector<test::NamedTensor> outputs;
  std::optional<std::pair<std::string, double>> double_attribute;
};

class ObliviousGroupAggTest
    : public testing::TestWithParam<
          std::tuple<test::SpuRuntimeTestCase, ObliviousGroupAggTestCase>> {
 protected:
  void SetUp() override { RegisterAllOps(); }

  static pb::ExecNode MakeExecNode(const ObliviousGroupAggTestCase& tc);
};

TEST_P(ObliviousGroupAggTest, works) {
  // Given
  auto parm = GetParam();
  auto tc = std::get<1>(parm);
  auto node = MakeExecNode(tc);
  auto sessions = test::MakeMultiPCSession(std::get<0>(parm));

  std::vector<ExecContext> exec_ctxs;
  exec_ctxs.reserve(sessions.size());
  for (auto& session : sessions) {
    exec_ctxs.emplace_back(node, session.get());
  }

  // feed inputs
  std::vector<ExecContext*> ctx_ptrs;
  ctx_ptrs.reserve(exec_ctxs.size());
  for (auto& exec_ctx : exec_ctxs) {
    ctx_ptrs.emplace_back(&exec_ctx);
  }
  test::FeedInputsAsSecret(ctx_ptrs, tc.inputs);
  test::FeedInputsAsSecret(ctx_ptrs, {tc.group});

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
/// ObliviousGroupAggTest impl
/// ===================

pb::ExecNode ObliviousGroupAggTest::MakeExecNode(
    const ObliviousGroupAggTestCase& tc) {
  test::ExecNodeBuilder builder(tc.op_type);

  if (tc.double_attribute.has_value()) {
    builder.AddDoubleAttr(tc.double_attribute->first,
                          tc.double_attribute->second);
  }
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
        testing::Values(test::SpuRuntimeTestCase{spu::ProtocolKind::CHEETAH, 2},
                        test::SpuRuntimeTestCase{spu::ProtocolKind::SEMI2K, 2},
                        test::SpuRuntimeTestCase{spu::ProtocolKind::SEMI2K, 3},
                        test::SpuRuntimeTestCase{spu::ProtocolKind::ABY3, 3}),
        testing::Values(
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupSum::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFrom(arrow::boolean(), "[true, false, true]"))},
                .group = test::NamedTensor("group", TensorFrom(arrow::boolean(),
                                                               "[0, 0, 1]")),
                .outputs = {test::NamedTensor("out", TensorFrom(arrow::int64(),
                                                                "[1, 1, 2]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupSum::kOpType,
                .inputs = {test::NamedTensor("in", TensorFrom(arrow::boolean(),
                                                              "[true]"))},
                .group = test::NamedTensor("group",
                                           TensorFrom(arrow::boolean(), "[0]")),
                .outputs = {test::NamedTensor("out", TensorFrom(arrow::int64(),
                                                                "[1]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupSum::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFrom(arrow::int64(), "[1, 1, 1, 1, 1]"))},
                .group = test::NamedTensor(
                    "group", TensorFrom(arrow::boolean(), "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFrom(arrow::int64(), "[1, 1, 2, 3, 1]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupSum::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFrom(arrow::float32(),
                                     "[-3.14, 1.1, 10, 100, 31415.9]"))},
                .group = test::NamedTensor(
                    "group", TensorFrom(arrow::boolean(), "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFrom(arrow::float32(),
                                      "[-3.14, 1.1, 11.1, 111.1, 31415.9]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupSum::kOpType,
                .inputs = {test::NamedTensor("in", TensorFrom(arrow::float32(),
                                                              "[]"))},
                .group = test::NamedTensor("group",
                                           TensorFrom(arrow::boolean(), "[]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFrom(arrow::float32(), "[]"))}})),
    TestParamNameGenerator(ObliviousGroupAggTest));

// =====================
// TEST_SUITE: ObliviousGroupCount
// =====================

INSTANTIATE_TEST_SUITE_P(
    ObliviousGroupCountTest, ObliviousGroupAggTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupCount::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFrom(arrow::int64(), "[1, 2, 3, 4, 5]"))},
                .group = test::NamedTensor(
                    "group", TensorFrom(arrow::boolean(), "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFrom(arrow::int64(), "[1, 1, 2, 3, 1]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupCount::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFrom(arrow::float32(),
                                     "[-3.14, 1.1, 10, 100, 31415.9]"))},
                .group = test::NamedTensor(
                    "group", TensorFrom(arrow::boolean(), "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFrom(arrow::int64(), "[1, 1, 2, 3, 1]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupCount::kOpType,
                .inputs = {test::NamedTensor("in", TensorFrom(arrow::float32(),
                                                              "[]"))},
                .group = test::NamedTensor("group",
                                           TensorFrom(arrow::boolean(), "[]")),
                .outputs = {test::NamedTensor("out", TensorFrom(arrow::int64(),
                                                                "[]"))}})),
    TestParamNameGenerator(ObliviousGroupAggTest));

// =====================
// TEST_SUITE: ObliviousGroupAvg
// =====================

INSTANTIATE_TEST_SUITE_P(
    ObliviousGroupAvgTest, ObliviousGroupAggTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupAvg::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFrom(arrow::int64(), "[1, 2, 3, 4, 5]"))},
                .group = test::NamedTensor(
                    "group", TensorFrom(arrow::boolean(), "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFrom(arrow::float64(), "[1, 2, 2.5, 3, 5]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupAvg::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFrom(arrow::float32(),
                                     "[-3.14, 1.3, 10, 100, 314.08]"))},
                .group = test::NamedTensor(
                    "group", TensorFrom(arrow::boolean(), "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFrom(arrow::float64(),
                                      "[-3.14, 1.3, 5.65, 37.1, 314.08]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupAvg::kOpType,
                .inputs = {test::NamedTensor("in", TensorFrom(arrow::float32(),
                                                              "[]"))},
                .group = test::NamedTensor("group",
                                           TensorFrom(arrow::boolean(), "[]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFrom(arrow::float64(), "[]"))}})),
    TestParamNameGenerator(ObliviousGroupAggTest));

// =====================
// TEST_SUITE: ObliviousGroupMax
// =====================

INSTANTIATE_TEST_SUITE_P(
    ObliviousGroupMaxTest, ObliviousGroupAggTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupMax::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFrom(arrow::int64(), "[1, 2, 3, 4, 5]"))},
                .group = test::NamedTensor(
                    "group", TensorFrom(arrow::boolean(), "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFrom(arrow::int64(), "[1, 2, 3, 4, 5]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupMax::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFrom(arrow::float32(),
                                     "[-3.14, 1.3, 10, 100, 314.08]"))},
                .group = test::NamedTensor(
                    "group", TensorFrom(arrow::boolean(), "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFrom(arrow::float32(),
                                      "[-3.14, 1.3, 10, 100, 314.08]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupMax::kOpType,
                .inputs = {test::NamedTensor("in", TensorFrom(arrow::float32(),
                                                              "[]"))},
                .group = test::NamedTensor("group",
                                           TensorFrom(arrow::boolean(), "[]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFrom(arrow::float32(), "[]"))}})),
    TestParamNameGenerator(ObliviousGroupAggTest));

// =====================
// TEST_SUITE: ObliviousGroupMin
// =====================

INSTANTIATE_TEST_SUITE_P(
    ObliviousGroupMinTest, ObliviousGroupAggTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupMin::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFrom(arrow::int64(), "[1, 2, 3, 4, 5]"))},
                .group = test::NamedTensor(
                    "group", TensorFrom(arrow::boolean(), "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFrom(arrow::int64(), "[1, 2, 2, 2, 5]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupMin::kOpType,
                .inputs =
                    {test::NamedTensor("in_a", TensorFrom(arrow::int64(),
                                                          "[1, 2, 3, 4, 5]")),
                     test::NamedTensor(
                         "in_b", TensorFrom(arrow::float32(),
                                            "[-3.14, 1.3, 10, 100, 314.08]"))},
                .group = test::NamedTensor(
                    "group", TensorFrom(arrow::boolean(), "[1, 0, 0, 1, 1]")),
                .outputs = {test::NamedTensor("out_a",
                                              TensorFrom(arrow::int64(),
                                                         "[1, 2, 2, 2, 5]")),
                            test::NamedTensor("out_b",
                                              TensorFrom(arrow::float32(),
                                                         "[-3.14, 1.3, 1.3, "
                                                         "1.3, 314.08]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousGroupMin::kOpType,
                .inputs = {test::NamedTensor("in", TensorFrom(arrow::float32(),
                                                              "[]"))},
                .group = test::NamedTensor("group",
                                           TensorFrom(arrow::boolean(), "[]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFrom(arrow::float32(), "[]"))}})),
    TestParamNameGenerator(ObliviousGroupAggTest));

INSTANTIATE_TEST_SUITE_P(
    ObliviousPercentRankTest, ObliviousGroupAggTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            ObliviousGroupAggTestCase{
                .op_type = ObliviousPercentRank::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFrom(arrow::int64(), "[1, 2, 3, 4, 5]"))},
                .group = test::NamedTensor(
                    "group", TensorFrom(arrow::boolean(), "[1, 0, 0, 0, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFrom(arrow::float64(),
                                      "[1.0, 0.25, 0.5, 0.75, 1.0]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousPercentRank::kOpType,
                .inputs = {test::NamedTensor("in", TensorFrom(arrow::int64(),
                                                              "[1]"))},
                .group = test::NamedTensor("group",
                                           TensorFrom(arrow::boolean(), "[1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFrom(arrow::float64(), "[1.0]"))}},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousPercentRank::kOpType,
                .inputs = {test::NamedTensor("in", TensorFrom(arrow::int64(),
                                                              "[1,2]"))},
                .group = test::NamedTensor("group", TensorFrom(arrow::boolean(),
                                                               "[1,1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFrom(arrow::float64(), "[1.0,1.0]"))}})),
    TestParamNameGenerator(ObliviousGroupAggTest));

static ObliviousGroupAggTestCase GenerateObliviousPercentileDiscTestCase(
    double percentile, std::vector<int> groups) {
  std::vector<int> in;
  std::vector<int> out;
  std::vector<int> group;
  for (const auto group_size : groups) {
    int count = 0;
    std::vector<int> current_in;
    while (count < group_size) {
      if (count == group_size - 1) {
        group.push_back(1);
      } else {
        group.push_back(0);
      }
      current_in.push_back(count);
      count++;
    }

    int index = static_cast<int>(std::ceil(percentile * group_size)) - 1;
    index = std::min(group_size - 1, index);
    int value = current_in[index];
    for (int i = 0; i < index; i++) {
      out.push_back(0);
    }

    for (int i = index; i < group_size; i++) {
      out.push_back(value);
    }
    in.insert(in.end(), current_in.begin(), current_in.end());
  }

  std::string in_str = "[";
  std::string group_str = "[";
  std::string out_str = "[";
  for (size_t i = 0; i < in.size(); i++) {
    in_str += std::to_string(in[i]);
    group_str += std::to_string(group[i]);
    out_str += std::to_string(out[i]);
    if (i != in.size() - 1) {
      in_str += ",";
      group_str += ",";
      out_str += ",";
    }
  }

  in_str += "]";
  group_str += "]";
  out_str += "]";

  ObliviousGroupAggTestCase tc{
      .op_type = ObliviousPercentileDisc::kOpType,
      .inputs = {test::NamedTensor("in", TensorFrom(arrow::int64(), in_str))},
      .group =
          test::NamedTensor("group", TensorFrom(arrow::boolean(), group_str)),
      .outputs = {test::NamedTensor("out",
                                    TensorFrom(arrow::int64(), out_str))},
      .double_attribute =
          std::make_pair(ObliviousPercentileDisc::kPercent, percentile)};
  ;

  return tc;
}

INSTANTIATE_TEST_SUITE_P(
    ObliviousPercentileDiscTest, ObliviousGroupAggTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            ObliviousGroupAggTestCase{
                .op_type = ObliviousPercentileDisc::kOpType,
                .inputs = {test::NamedTensor(
                    "in",
                    TensorFrom(arrow::int64(), "[1, 2, 3, 4, 5, 6, 7, 8]"))},
                .group = test::NamedTensor(
                    "group",
                    TensorFrom(arrow::boolean(), "[1, 0, 0, 0, 1, 0, 0, 1]")),
                .outputs = {test::NamedTensor(
                    "out",
                    TensorFrom(
                        arrow::int64(),
                        "[1, 0, 3, 3, 3, 0, 7, 7]"))},  // ceil(0.5*length)
                                                        // - 1
                .double_attribute =
                    std::make_pair(ObliviousPercentileDisc::kPercent, 0.5)},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousPercentileDisc::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFrom(arrow::int64(), "[1, 2, 3, 4, 5]"))},
                .group = test::NamedTensor(
                    "group", TensorFrom(arrow::boolean(), "[1, 0, 0, 0, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFrom(arrow::int64(), "[1, 2, 2, 2, 2]"))},
                .double_attribute =
                    std::make_pair(ObliviousPercentileDisc::kPercent, 0)},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousPercentileDisc::kOpType,
                .inputs = {test::NamedTensor(
                    "in", TensorFrom(arrow::int64(), "[1, 2, 3, 4, 5]"))},
                .group = test::NamedTensor(
                    "group", TensorFrom(arrow::boolean(), "[1, 0, 0, 0, 1]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFrom(arrow::int64(), "[1, 2, 3, 4, 5]"))},
                .double_attribute =
                    std::make_pair(ObliviousPercentileDisc::kPercent, 1)},
            ObliviousGroupAggTestCase{
                .op_type = ObliviousPercentileDisc::kOpType,
                .inputs = {test::NamedTensor("in", TensorFrom(arrow::float32(),
                                                              "[]"))},
                .group = test::NamedTensor("group",
                                           TensorFrom(arrow::boolean(), "[]")),
                .outputs = {test::NamedTensor(
                    "out", TensorFrom(arrow::float32(), "[]"))},
                .double_attribute =
                    std::make_pair(ObliviousPercentileDisc::kPercent, 0.5)},
            GenerateObliviousPercentileDiscTestCase(0.343, {10, 100}),
            GenerateObliviousPercentileDiscTestCase(0.43, {10, 100, 1234}),
            GenerateObliviousPercentileDiscTestCase(0.3, {10, 100, 3215}))),
    TestParamNameGenerator(ObliviousGroupAggTest));

}  // namespace scql::engine::op
