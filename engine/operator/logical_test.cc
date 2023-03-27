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

#include "engine/operator/logical.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_from_json.h"
#include "engine/operator/binary_test.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct NotTestCase {
  std::vector<test::NamedTensor> inputs;
  pb::TensorStatus input_status;
  std::vector<test::NamedTensor> outputs;
};

class NotTest : public testing::TestWithParam<
                    std::tuple<spu::ProtocolKind, NotTestCase>> {
 public:
  static pb::ExecNode MakeExecNode(const NotTestCase& tc);

  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const NotTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    NotBatchTest, NotTest,
    testing::Combine(
        testing::Values(spu::ProtocolKind::CHEETAH, spu::ProtocolKind::SEMI2K),
        testing::Values(
            NotTestCase{
                .inputs = {test::NamedTensor(
                    "x", TensorFromJSON(arrow::boolean(),
                                        "[true, false, null, false, false]"))},
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor(
                    "x_hat",
                    TensorFromJSON(arrow::boolean(),
                                   "[false, true, null, true, true]"))}},
            NotTestCase{
                .inputs = {test::NamedTensor(
                    "x", TensorFromJSON(arrow::boolean(),
                                        "[true, false, true, false, false]"))},
                .input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor(
                    "x_hat",
                    TensorFromJSON(arrow::boolean(),
                                   "[false, true, false, true, true]"))}}
            // ,
            // // testcase  with empty inputs
            // NotTestCase{.inputs = {test::NamedTensor(
            //                 "x", TensorFromJSON(arrow::boolean(), "[]"))},
            //             .input_status = pb::TENSORSTATUS_SECRET,
            //             .outputs = {test::NamedTensor(
            //                 "x_hat", TensorFromJSON(arrow::boolean(),
            //                 "[]"))}}
            )));

TEST_P(NotTest, Works) {
  auto parm = GetParam();
  auto tc = std::get<1>(parm);
  auto node = MakeExecNode(tc);
  std::vector<Session> sessions = test::Make2PCSession(std::get<0>(parm));

  ExecContext alice_ctx(node, &sessions[0]);
  ExecContext bob_ctx(node, &sessions[1]);

  FeedInputs({&alice_ctx, &bob_ctx}, tc);

  if (tc.input_status == pb::TENSORSTATUS_PRIVATE) {
    Not alice_op;
    EXPECT_NO_THROW({ alice_op.Run(&alice_ctx); });

    auto tensor_table = alice_ctx.GetTensorTable();
    for (const auto& named_tensor : tc.outputs) {
      auto t = tensor_table->GetTensor(named_tensor.name);
      ASSERT_TRUE(t != nullptr) << named_tensor.name << " not found";

      auto expect_out_arr = named_tensor.tensor->ToArrowChunkedArray();
      auto actual_out_arr = t->ToArrowChunkedArray();
      EXPECT_TRUE(actual_out_arr->ApproxEquals(*expect_out_arr))
          << "\nexpect result = " << expect_out_arr->ToString()
          << "\nbut actual got result = " << actual_out_arr->ToString();
    }
  } else {
    test::OperatorTestRunner<Not> alice;
    test::OperatorTestRunner<Not> bob;

    alice.Start(&alice_ctx);
    bob.Start(&bob_ctx);

    EXPECT_NO_THROW({ alice.Wait(); });
    EXPECT_NO_THROW({ bob.Wait(); });

    for (const auto& named_tensor : tc.outputs) {
      TensorPtr t = nullptr;
      EXPECT_NO_THROW({
        t = test::RevealSecret({&alice_ctx, &bob_ctx}, named_tensor.name);
      });
      ASSERT_TRUE(t != nullptr);
      auto expect_out_arr = named_tensor.tensor->ToArrowChunkedArray();
      auto actual_out_arr = t->ToArrowChunkedArray();
      EXPECT_TRUE(actual_out_arr->Equals(expect_out_arr))
          << "\nexpect result = " << expect_out_arr->ToString()
          << "\nbut actual got result = " << actual_out_arr->ToString();
    }
  }
}

pb::ExecNode NotTest::MakeExecNode(const NotTestCase& tc) {
  test::ExecNodeBuilder builder(Not::kOpType);

  builder.SetNodeName("not-test");
  std::vector<pb::Tensor> inputs;
  for (const auto& named_tensor : tc.inputs) {
    auto t = test::MakeTensorReference(
        named_tensor.name, named_tensor.tensor->Type(), tc.input_status);
    inputs.push_back(std::move(t));
  }
  builder.AddInput(Not::kIn, inputs);

  std::vector<pb::Tensor> outputs;
  for (size_t i = 0; i < tc.outputs.size(); ++i) {
    auto t = test::MakeTensorAs(tc.outputs[i].name, inputs[i]);
    outputs.push_back(std::move(t));
  }
  builder.AddOutput(Not::kOut, outputs);

  return builder.Build();
}

void NotTest::FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const NotTestCase& tc) {
  if (tc.input_status == pb::TENSORSTATUS_PRIVATE) {
    test::FeedInputsAsPrivate(ctxs[0], tc.inputs);
  } else {
    test::FeedInputsAsSecret(ctxs, tc.inputs);
  }
}

// testcases for LogicalAnd & LogicalOr

INSTANTIATE_TEST_SUITE_P(
    LogicalBatchTest, BinaryComputeInSecretTest,
    testing::Combine(
        testing::Values(spu::ProtocolKind::CHEETAH, spu::ProtocolKind::SEMI2K),
        testing::Values(
            BinaryTestCase{
                .op_type = LogicalAnd::kOpType,
                .left_inputs =
                    {test::NamedTensor(
                         "x1", TensorFromJSON(arrow::boolean(),
                                              "[true, false, true, false]")),
                     test::NamedTensor(
                         "x2", TensorFromJSON(arrow::boolean(),
                                              "[false, false, true, false]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs =
                    {test::NamedTensor(
                         "y1", TensorFromJSON(arrow::boolean(),
                                              "[true, true, false, false]")),
                     test::NamedTensor(
                         "y2", TensorFromJSON(arrow::boolean(),
                                              "[true, true, true, true]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs =
                    {test::NamedTensor(
                         "z1", TensorFromJSON(arrow::boolean(),
                                              "[true, false, false, false]")),
                     test::NamedTensor(
                         "z2", TensorFromJSON(arrow::boolean(),
                                              "[false, false, true, false]"))},
                .output_status = pb::TENSORSTATUS_SECRET},
            BinaryTestCase{
                .op_type = LogicalOr::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x1", TensorFromJSON(arrow::boolean(),
                                         "[true, false, true, false]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y1", TensorFromJSON(arrow::boolean(),
                                         "[true, true, false, false]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor(
                    "z1", TensorFromJSON(arrow::boolean(),
                                         "[true, true, true, false]"))},
                .output_status = pb::TENSORSTATUS_SECRET},
            // testcase with empty inputs
            BinaryTestCase{.op_type = LogicalAnd::kOpType,
                           .left_inputs = {test::NamedTensor(
                               "x1", TensorFromJSON(arrow::boolean(), "[]"))},
                           .left_input_status = pb::TENSORSTATUS_SECRET,
                           .right_inputs = {test::NamedTensor(
                               "y1", TensorFromJSON(arrow::boolean(), "[]"))},
                           .right_input_status = pb::TENSORSTATUS_SECRET,
                           .outputs = {test::NamedTensor(
                               "z1", TensorFromJSON(arrow::boolean(), "[]"))},
                           .output_status = pb::TENSORSTATUS_SECRET},
            BinaryTestCase{.op_type = LogicalOr::kOpType,
                           .left_inputs = {test::NamedTensor(
                               "x1", TensorFromJSON(arrow::boolean(), "[]"))},
                           .left_input_status = pb::TENSORSTATUS_SECRET,
                           .right_inputs = {test::NamedTensor(
                               "y1", TensorFromJSON(arrow::boolean(), "[]"))},
                           .right_input_status = pb::TENSORSTATUS_SECRET,
                           .outputs = {test::NamedTensor(
                               "z1", TensorFromJSON(arrow::boolean(), "[]"))},
                           .output_status = pb::TENSORSTATUS_SECRET})),
    TestParamNameGenerator(BinaryComputeInSecretTest));

INSTANTIATE_TEST_SUITE_P(
    LogicalBatchTest, BinaryComputeInPlainTest,
    testing::Combine(
        testing::Values(spu::ProtocolKind::CHEETAH, spu::ProtocolKind::SEMI2K),
        testing::Values(
            BinaryTestCase{
                .op_type = LogicalAnd::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x1", TensorFromJSON(arrow::boolean(),
                                         "[true, false, true, null]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y1", TensorFromJSON(arrow::boolean(),
                                         "[true, true, false, false]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor(
                    "z1", TensorFromJSON(arrow::boolean(),
                                         "[true, false, false, null]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE},
            BinaryTestCase{
                .op_type = LogicalOr::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x1", TensorFromJSON(arrow::boolean(),
                                         "[true, false, true, false]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y1", TensorFromJSON(arrow::boolean(),
                                         "[true, null, false, false]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor(
                    "z1", TensorFromJSON(arrow::boolean(),
                                         "[true, null, true, false]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE})),
    TestParamNameGenerator(BinaryComputeInPlainTest));

}  // namespace scql::engine::op