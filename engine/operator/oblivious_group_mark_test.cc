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

#include "engine/operator/oblivious_group_mark.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct ObliviousGroupMarkTestCase {
  std::vector<test::NamedTensor> inputs;
  test::NamedTensor output;
};

class ObliviousGroupMarkTest
    : public testing::TestWithParam<
          std::tuple<test::SpuRuntimeTestCase, ObliviousGroupMarkTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const ObliviousGroupMarkTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    ObliviousGroupMarkBatchTest, ObliviousGroupMarkTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            ObliviousGroupMarkTestCase{
                .inputs = {test::NamedTensor(
                    "in", TensorFrom(arrow::int64(), "[0, 1, 1, 1, 2]"))},
                .output = test::NamedTensor(
                    "out", TensorFrom(arrow::boolean(), "[1, 0, 0, 1, 1]"))},
            ObliviousGroupMarkTestCase{
                .inputs = {test::NamedTensor("in_a",
                                             TensorFrom(arrow::int64(),
                                                        "[0, 0, 1, 1, 1, 1]")),
                           test::NamedTensor(
                               "in_b",
                               TensorFrom(arrow::float32(),
                                          "[-1, 0, 0, 3.14, 3.14, 3.14]"))},
                .output = test::NamedTensor(
                    "out", TensorFrom(arrow::boolean(), "[1, 1, 1, 0, 0, 1]"))},
            ObliviousGroupMarkTestCase{
                .inputs = {test::NamedTensor("in", TensorFrom(arrow::int64(),
                                                              "[1]"))},
                .output = test::NamedTensor("out", TensorFrom(arrow::boolean(),
                                                              "[1]"))},
            ObliviousGroupMarkTestCase{
                .inputs = {test::NamedTensor("in",
                                             TensorFrom(arrow::int64(), "[]"))},
                .output = test::NamedTensor("out", TensorFrom(arrow::boolean(),
                                                              "[]"))})),
    TestParamNameGenerator(ObliviousGroupMarkTest));

TEST_P(ObliviousGroupMarkTest, works) {
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

  // When
  EXPECT_NO_THROW(test::RunAsync<ObliviousGroupMark>(ctx_ptrs));

  // Then
  TensorPtr actual_output = nullptr;
  EXPECT_NO_THROW(
      { actual_output = test::RevealSecret(ctx_ptrs, tc.output.name); });
  ASSERT_TRUE(actual_output != nullptr);
  auto actual_arr = actual_output->ToArrowChunkedArray();
  auto expect_arr = tc.output.tensor->ToArrowChunkedArray();
  EXPECT_TRUE(actual_arr->ApproxEquals(
      *expect_arr, arrow::EqualOptions::Defaults().atol(0.001)))
      << "\nexpect result = " << expect_arr->ToString()
      << "\nbut actual got result = " << actual_arr->ToString();
}

/// ===================
/// ObliviousGroupMarkTest impl
/// ===================

pb::ExecNode ObliviousGroupMarkTest::MakeExecNode(
    const ObliviousGroupMarkTestCase& tc) {
  test::ExecNodeBuilder builder(ObliviousGroupMark::kOpType);

  builder.SetNodeName("oblivious-group-mark-test");

  std::vector<pb::Tensor> inputs;
  for (const auto& named_tensor : tc.inputs) {
    pb::Tensor t = test::MakeSecretTensorReference(named_tensor.name,
                                                   named_tensor.tensor->Type());
    inputs.push_back(std::move(t));
  }
  builder.AddInput(ObliviousGroupMark::kIn, inputs);

  pb::Tensor output =
      test::MakeSecretTensorReference(tc.output.name, tc.output.tensor->Type());
  builder.AddOutput(ObliviousGroupMark::kOut, {output});

  return builder.Build();
}

}  // namespace scql::engine::op