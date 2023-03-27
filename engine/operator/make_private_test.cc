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

#include "engine/core/tensor_from_json.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct MakePrivateTestCase {
  std::vector<test::NamedTensor> inputs;
  std::string reveal_to;
  std::vector<std::string> output_names;
};

class MakePrivateTest
    : public testing::TestWithParam<
          std::tuple<spu::ProtocolKind, MakePrivateTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const MakePrivateTestCase& tc);
  static void FeedInputs(std::vector<ExecContext*> ctxs,
                         const MakePrivateTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    MakePrivateBatchTest, MakePrivateTest,
    testing::Combine(
        testing::Values(spu::ProtocolKind::CHEETAH, spu::ProtocolKind::SEMI2K),
        testing::Values(
            MakePrivateTestCase{
                .inputs =
                    {test::NamedTensor(
                         "x", TensorFromJSON(arrow::utf8(),
                                             R"json(["A", "B", "C"])json")),
                     test::NamedTensor(
                         "y", TensorFromJSON(arrow::utf8(),
                                             R"json(["X", "Y", "Z"])json"))},
                .reveal_to = "alice",
                .output_names = {"x_hat", "y_hat"}},
            MakePrivateTestCase{
                .inputs = {test::NamedTensor(
                               "x",
                               TensorFromJSON(arrow::int64(),
                                              "[42, -121, 9527, 1498672]")),
                           test::NamedTensor(
                               "y", TensorFromJSON(arrow::float32(),
                                                   "[1.234, 0, -1.5, 2.75]"))},
                .reveal_to = "alice",
                .output_names = {"x_hat", "y_hat"}},
            MakePrivateTestCase{
                .inputs = {test::NamedTensor(
                               "x",
                               TensorFromJSON(arrow::int64(),
                                              "[42, -121, 9527, 1498672]")),
                           test::NamedTensor(
                               "y", TensorFromJSON(arrow::float32(),
                                                   "[1.234, 0, -1.5, 2.75]"))},
                .reveal_to = "bob",
                .output_names = {"x_hat", "y_hat"}})),
    TestParamNameGenerator(MakePrivateTest));

TEST_P(MakePrivateTest, Works) {
  // Given
  auto parm = GetParam();
  auto tc = std::get<1>(parm);
  auto node = MakeExecNode(tc);
  std::vector<Session> sessions = test::Make2PCSession(std::get<0>(parm));

  ExecContext alice_ctx(node, &sessions[0]);
  ExecContext bob_ctx(node, &sessions[1]);

  // feed inputs
  FeedInputs(std::vector<ExecContext*>{&alice_ctx, &bob_ctx}, tc);

  // When
  MakePrivate alice_op, bob_op;
  test::OpAsyncRunner alice(&alice_op);
  test::OpAsyncRunner bob(&bob_op);

  alice.Start(&alice_ctx);
  bob.Start(&bob_ctx);

  // Then
  EXPECT_NO_THROW({ alice.Wait(); });
  EXPECT_NO_THROW({ bob.Wait(); });

  auto tensor_table = alice_ctx.GetSession()->GetTensorTable();
  if (tc.reveal_to == bob_ctx.GetSession()->SelfPartyCode()) {
    tensor_table = bob_ctx.GetSession()->GetTensorTable();
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
    auto t = test::MakeSecretTensorReference(named_tensor.name,
                                             named_tensor.tensor->Type());
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
  test::FeedInputsAsSecret(ctxs, tc.inputs);
}

}  // namespace scql::engine::op