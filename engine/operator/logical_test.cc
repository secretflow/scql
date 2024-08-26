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

#include "engine/core/tensor_constructor.h"
#include "engine/operator/binary_test.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct NotTestCase {
  std::vector<test::NamedTensor> inputs;
  pb::TensorStatus input_status;
  std::vector<test::NamedTensor> outputs;
};

class NotTest : public testing::TestWithParam<
                    std::tuple<test::SpuRuntimeTestCase, NotTestCase>> {
 public:
  static pb::ExecNode MakeExecNode(const NotTestCase& tc);

  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const NotTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    NotBatchTest, NotTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            NotTestCase{
                .inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::boolean(),
                                    "[true, false, null, false, false]"))},
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor(
                    "x_hat", TensorFrom(arrow::boolean(),
                                        "[false, true, null, true, true]"))}},
            NotTestCase{
                .inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::boolean(),
                                    "[true, false, true, false, false]"))},
                .input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor(
                    "x_hat", TensorFrom(arrow::boolean(),
                                        "[false, true, false, true, true]"))}}
            // ,
            // // testcase  with empty inputs
            // NotTestCase{.inputs = {test::NamedTensor(
            //                 "x", TensorFrom(arrow::boolean(), "[]"))},
            //             .input_status = pb::TENSORSTATUS_SECRET,
            //             .outputs = {test::NamedTensor(
            //                 "x_hat", TensorFrom(arrow::boolean(),
            //                 "[]"))}}
            )));

TEST_P(NotTest, Works) {
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

  if (tc.input_status == pb::TENSORSTATUS_PRIVATE) {
    Not alice_op;
    EXPECT_NO_THROW({ alice_op.Run(ctx_ptrs[0]); });

    auto tensor_table = ctx_ptrs[0]->GetTensorTable();
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
    EXPECT_NO_THROW(test::RunAsync<Not>(ctx_ptrs));

    for (const auto& named_tensor : tc.outputs) {
      TensorPtr t = nullptr;
      EXPECT_NO_THROW({ t = test::RevealSecret(ctx_ptrs, named_tensor.name); });
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
        test::SpuTestValuesMultiPC,
        testing::Values(
            BinaryTestCase{
                .op_type = LogicalAnd::kOpType,
                .left_inputs =
                    {test::NamedTensor(
                         "x1", TensorFrom(arrow::boolean(),
                                          "[true, false, true, false]")),
                     test::NamedTensor(
                         "x2", TensorFrom(arrow::boolean(),
                                          "[false, false, true, false]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs =
                    {test::NamedTensor(
                         "y1", TensorFrom(arrow::boolean(),
                                          "[true, true, false, false]")),
                     test::NamedTensor("y2",
                                       TensorFrom(arrow::boolean(),
                                                  "[true, true, true, true]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor(
                                "z1",
                                TensorFrom(arrow::boolean(),
                                           "[true, false, false, false]")),
                            test::NamedTensor(
                                "z2",
                                TensorFrom(arrow::boolean(),
                                           "[false, false, true, false]"))},
                .output_status = pb::TENSORSTATUS_SECRET},
            BinaryTestCase{.op_type = LogicalOr::kOpType,
                           .left_inputs = {test::NamedTensor(
                               "x1", TensorFrom(arrow::boolean(),
                                                "[true, false, true, false]"))},
                           .left_input_status = pb::TENSORSTATUS_SECRET,
                           .right_inputs = {test::NamedTensor(
                               "y1", TensorFrom(arrow::boolean(),
                                                "[true, true, false, false]"))},
                           .right_input_status = pb::TENSORSTATUS_SECRET,
                           .outputs = {test::NamedTensor(
                               "z1", TensorFrom(arrow::boolean(),
                                                "[true, true, true, false]"))},
                           .output_status = pb::TENSORSTATUS_SECRET},
            // testcase with empty inputs
            BinaryTestCase{.op_type = LogicalAnd::kOpType,
                           .left_inputs = {test::NamedTensor(
                               "x1", TensorFrom(arrow::boolean(), "[]"))},
                           .left_input_status = pb::TENSORSTATUS_SECRET,
                           .right_inputs = {test::NamedTensor(
                               "y1", TensorFrom(arrow::boolean(), "[]"))},
                           .right_input_status = pb::TENSORSTATUS_SECRET,
                           .outputs = {test::NamedTensor(
                               "z1", TensorFrom(arrow::boolean(), "[]"))},
                           .output_status = pb::TENSORSTATUS_SECRET},
            BinaryTestCase{.op_type = LogicalOr::kOpType,
                           .left_inputs = {test::NamedTensor(
                               "x1", TensorFrom(arrow::boolean(), "[]"))},
                           .left_input_status = pb::TENSORSTATUS_SECRET,
                           .right_inputs = {test::NamedTensor(
                               "y1", TensorFrom(arrow::boolean(), "[]"))},
                           .right_input_status = pb::TENSORSTATUS_SECRET,
                           .outputs = {test::NamedTensor(
                               "z1", TensorFrom(arrow::boolean(), "[]"))},
                           .output_status = pb::TENSORSTATUS_SECRET})),
    TestParamNameGenerator(BinaryComputeInSecretTest));

INSTANTIATE_TEST_SUITE_P(
    LogicalBatchTest, BinaryComputeInPlainTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            BinaryTestCase{.op_type = LogicalAnd::kOpType,
                           .left_inputs = {test::NamedTensor(
                               "x1", TensorFrom(arrow::boolean(),
                                                "[true, false, true, null]"))},
                           .left_input_status = pb::TENSORSTATUS_PRIVATE,
                           .right_inputs = {test::NamedTensor(
                               "y1", TensorFrom(arrow::boolean(),
                                                "[true, true, null, false]"))},
                           .right_input_status = pb::TENSORSTATUS_PRIVATE,
                           .outputs = {test::NamedTensor(
                               "z1", TensorFrom(arrow::boolean(),
                                                "[true, false, null, false]"))},
                           .output_status = pb::TENSORSTATUS_PRIVATE},
            BinaryTestCase{.op_type = LogicalOr::kOpType,
                           .left_inputs = {test::NamedTensor(
                               "x1", TensorFrom(arrow::boolean(),
                                                "[true, false, true, null]"))},
                           .left_input_status = pb::TENSORSTATUS_PRIVATE,
                           .right_inputs = {test::NamedTensor(
                               "y1", TensorFrom(arrow::boolean(),
                                                "[true, true, null, false]"))},
                           .right_input_status = pb::TENSORSTATUS_PRIVATE,
                           .outputs = {test::NamedTensor(
                               "z1", TensorFrom(arrow::boolean(),
                                                "[true, true, true, null]"))},
                           .output_status = pb::TENSORSTATUS_PRIVATE})),
    TestParamNameGenerator(BinaryComputeInPlainTest));

}  // namespace scql::engine::op