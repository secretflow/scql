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

#include "engine/operator/make_private.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct MakePrivateTestCase {
  std::vector<test::NamedTensor> inputs;
  pb::TensorStatus input_status;
  std::string reveal_to;
  std::vector<std::string> output_names;
};

class MakePrivateTest
    : public testing::TestWithParam<
          std::tuple<test::SpuRuntimeTestCase, MakePrivateTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const MakePrivateTestCase& tc);
  static void FeedInputs(std::vector<ExecContext*> ctxs,
                         const MakePrivateTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    MakePrivateBatchTest, MakePrivateTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            MakePrivateTestCase{
                .inputs = {test::NamedTensor(
                               "x", TensorFrom(arrow::large_utf8(),
                                               R"json(["A", "B", "C"])json")),
                           test::NamedTensor(
                               "y", TensorFrom(arrow::int64(),
                                               "[42, -121, 9527, 1498672]"))},
                .input_status = pb::TENSORSTATUS_PUBLIC,
                .reveal_to = "bob",
                .output_names = {"x_hat", "y_hat"}},
            MakePrivateTestCase{
                .inputs =
                    {test::NamedTensor("x",
                                       TensorFrom(arrow::int64(),
                                                  "[42, -121, 9527, 1498672]")),
                     test::NamedTensor("y",
                                       TensorFrom(arrow::float32(),
                                                  "[1.234, 0, -1.5, 2.75]")),
                     test::NamedTensor(
                         "z", TensorFrom(arrow::large_utf8(),
                                         R"json(["X", "Y", "ZZZ"])json")),
                     test::NamedTensor(
                         "k",
                         TensorFrom(arrow::boolean(),
                                    R"json([true, false, false, false])json"))},
                .input_status = pb::TENSORSTATUS_SECRET,
                .reveal_to = "alice",
                .output_names = {"x_hat", "y_hat", "z_hat", "k_hat"}},
            MakePrivateTestCase{
                .inputs = {test::NamedTensor(
                               "x", TensorFrom(arrow::int64(),
                                               "[42, -121, 9527, 1498672]")),
                           test::NamedTensor(
                               "y", TensorFrom(arrow::float32(),
                                               "[1.234, 0, -1.5, 2.75]"))},
                .input_status = pb::TENSORSTATUS_SECRET,
                .reveal_to = "bob",
                .output_names = {"x_hat", "y_hat"}})),
    TestParamNameGenerator(MakePrivateTest));

TEST_P(MakePrivateTest, Works) {
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
  FeedInputs(ctx_ptrs, tc);

  // When
  EXPECT_NO_THROW(test::RunAsync<MakePrivate>(ctx_ptrs));

  // Then
  auto tensor_table = exec_ctxs[0].GetSession()->GetTensorTable();
  for (size_t idx = 1; idx < exec_ctxs.size(); ++idx) {
    if (tc.reveal_to == exec_ctxs[idx].GetSession()->SelfPartyCode()) {
      tensor_table = exec_ctxs[idx].GetSession()->GetTensorTable();
    }
  }
  for (size_t i = 0; i < tc.output_names.size(); ++i) {
    auto tensor = tensor_table->GetTensor(tc.output_names[i]);
    EXPECT_TRUE(tensor != nullptr);

    EXPECT_TRUE(tensor->ToArrowChunkedArray()->ApproxEquals(
        *tc.inputs[i].tensor->ToArrowChunkedArray()));
  }
}

/// =====================
/// MakePrivateTest impl
/// =====================

pb::ExecNode MakePrivateTest::MakeExecNode(const MakePrivateTestCase& tc) {
  test::ExecNodeBuilder builder(MakePrivate::kOpType);

  builder.SetNodeName("make-private-test");
  builder.AddStringAttr(MakePrivate::kRevealToAttr, tc.reveal_to);

  std::vector<pb::Tensor> inputs;
  for (const auto& named_tensor : tc.inputs) {
    auto t = test::MakeTensorReference(
        named_tensor.name, named_tensor.tensor->Type(), tc.input_status);
    inputs.push_back(std::move(t));
  }
  builder.AddInput(MakePrivate::kIn, inputs);

  std::vector<pb::Tensor> outputs;
  for (size_t i = 0; i < tc.output_names.size(); ++i) {
    auto t = test::MakePrivateTensorReference(tc.output_names[i],
                                              inputs[i].elem_type());
    outputs.push_back(std::move(t));
  }
  builder.AddOutput(MakePrivate::kOut, outputs);

  return builder.Build();
}

void MakePrivateTest::FeedInputs(std::vector<ExecContext*> ctxs,
                                 const MakePrivateTestCase& tc) {
  if (tc.input_status == pb::TENSORSTATUS_SECRET) {
    test::FeedInputsAsSecret(ctxs, tc.inputs);
  } else {
    test::FeedInputsAsPublic(ctxs, tc.inputs);
  }
}

}  // namespace scql::engine::op