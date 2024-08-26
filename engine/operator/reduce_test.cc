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

#include "engine/operator/reduce.h"

#include "arrow/type.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/all_ops_register.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct ReduceTestCase {
  std::string op_type;
  pb::TensorStatus status;
  test::NamedTensor input;
  test::NamedTensor output;
};

class ReduceTest : public ::testing::TestWithParam<
                       std::tuple<test::SpuRuntimeTestCase, ReduceTestCase>> {
 protected:
  void SetUp() override { RegisterAllOps(); }

  static pb::ExecNode MakeExecNode(const ReduceTestCase& tc);
  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const ReduceTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    ReducePrivateTest, ReduceTest,
    ::testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            ReduceTestCase{.op_type = ReduceSum::kOpType,
                           .status = pb::TENSORSTATUS_PRIVATE,
                           .input = test::NamedTensor(
                               "x", TensorFrom(arrow::boolean(),
                                               "[true, false, true, null]")),
                           .output = test::NamedTensor(
                               "y", TensorFrom(arrow::uint64(), "[2]"))},
            ReduceTestCase{.op_type = ReduceSum::kOpType,
                           .status = pb::TENSORSTATUS_PRIVATE,
                           .input = test::NamedTensor(
                               "x", TensorFrom(arrow::int64(),
                                               "[1, 2, 3, 4, 5, 6, null]")),
                           .output = test::NamedTensor(
                               "y", TensorFrom(arrow::int64(), "[21]"))},
            ReduceTestCase{
                .op_type = ReduceSum::kOpType,
                .status = pb::TENSORSTATUS_PRIVATE,
                .input = test::NamedTensor(
                    "x",
                    TensorFrom(arrow::float64(),
                               "[0.1, 0.22, 0.33, 0.44, 0.55, 0.67, null]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float64(),
                                                            "[2.31]"))},
            ReduceTestCase{.op_type = ReduceAvg::kOpType,
                           .status = pb::TENSORSTATUS_PRIVATE,
                           .input = test::NamedTensor(
                               "x", TensorFrom(arrow::int64(),
                                               "[1, 2, 3, 4, 5, 6, null]")),
                           .output = test::NamedTensor(
                               "y", TensorFrom(arrow::float64(), "[3.5]"))},
            ReduceTestCase{
                .op_type = ReduceAvg::kOpType,
                .status = pb::TENSORSTATUS_PRIVATE,
                .input = test::NamedTensor(
                    "x", TensorFrom(arrow::float32(),
                                    "[1.75, 2.34, 4.12, 1.99, null]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float64(),
                                                            "[2.55]"))},
            ReduceTestCase{
                .op_type = ReduceMax::kOpType,
                .status = pb::TENSORSTATUS_PRIVATE,
                .input = test::NamedTensor(
                    "x", TensorFrom(arrow::int64(),
                                    "[1000, -2, 3345, 42, 5999, 60, null]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::int64(),
                                                            "[5999]"))},
            ReduceTestCase{
                .op_type = ReduceMax::kOpType,
                .status = pb::TENSORSTATUS_PRIVATE,
                .input = test::NamedTensor(
                    "x", TensorFrom(arrow::float32(),
                                    "[1.75, 2.34, 4.12, 1.99, null]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float32(),
                                                            "[4.12]"))},
            ReduceTestCase{
                .op_type = ReduceMin::kOpType,
                .status = pb::TENSORSTATUS_PRIVATE,
                .input = test::NamedTensor(
                    "x", TensorFrom(arrow::int64(),
                                    "[1000, -2, 3345, 42, 5999, 60, null]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::int64(),
                                                            "[-2]"))},
            ReduceTestCase{
                .op_type = ReduceMin::kOpType,
                .status = pb::TENSORSTATUS_PRIVATE,
                .input = test::NamedTensor(
                    "x", TensorFrom(arrow::float32(),
                                    "[1.75, 2.34, 4.12, 1.99, null]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float32(),
                                                            "[1.75]"))},
            ReduceTestCase{
                .op_type = ReduceCount::kOpType,
                .status = pb::TENSORSTATUS_PRIVATE,
                .input = test::NamedTensor(
                    "x", TensorFrom(arrow::float32(),
                                    "[1.75, 2.34, 4.12, 1.99, null]")),
                .output = test::NamedTensor("y",
                                            TensorFrom(arrow::int64(), "[4]"))},
            ReduceTestCase{
                .op_type = ReduceCount::kOpType,
                .status = pb::TENSORSTATUS_PRIVATE,
                .input = test::NamedTensor(
                    "x", TensorFrom(arrow::float32(),
                                    "[null, 2.34, null, 1.99, null]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::int64(),
                                                            "[2]"))})),
    TestParamNameGenerator(ReduceTest));

INSTANTIATE_TEST_SUITE_P(
    ReduceSecretTest, ReduceTest,
    ::testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            ReduceTestCase{
                .op_type = ReduceSum::kOpType,
                .status = pb::TENSORSTATUS_SECRET,
                .input = test::NamedTensor(
                    "x", TensorFrom(arrow::boolean(), "[true, false, true]")),
                .output = test::NamedTensor("y",
                                            TensorFrom(arrow::int64(), "[2]"))},
            ReduceTestCase{
                .op_type = ReduceSum::kOpType,
                .status = pb::TENSORSTATUS_SECRET,
                .input = test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[1, 2, 3, 4, 5, 6]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::int64(),
                                                            "[21]"))},
            ReduceTestCase{
                .op_type = ReduceSum::kOpType,
                .status = pb::TENSORSTATUS_SECRET,
                .input = test::NamedTensor(
                    "x", TensorFrom(arrow::float32(),
                                    "[0.1, 0.22, 0.33, 0.44, 0.55, 0.67]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float32(),
                                                            "[2.31]"))},
            ReduceTestCase{
                .op_type = ReduceAvg::kOpType,
                .status = pb::TENSORSTATUS_SECRET,
                .input = test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[1, 2, 3, 4, 5, 6]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float64(),
                                                            "[3.5]"))},
            ReduceTestCase{.op_type = ReduceAvg::kOpType,
                           .status = pb::TENSORSTATUS_SECRET,
                           .input = test::NamedTensor(
                               "x", TensorFrom(arrow::float32(),
                                               "[1.75, 2.34, 4.12, 1.99]")),
                           .output = test::NamedTensor(
                               "y", TensorFrom(arrow::float64(), "[2.55]"))},
            ReduceTestCase{
                .op_type = ReduceMax::kOpType,
                .status = pb::TENSORSTATUS_SECRET,
                .input = test::NamedTensor(
                    "x", TensorFrom(arrow::int64(),
                                    "[1000, -2, 3345, 42, 5999, 60]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::int64(),
                                                            "[5999]"))},
            ReduceTestCase{.op_type = ReduceMax::kOpType,
                           .status = pb::TENSORSTATUS_SECRET,
                           .input = test::NamedTensor(
                               "x", TensorFrom(arrow::float32(),
                                               "[1.75, 2.34, 4.12, 1.99]")),
                           .output = test::NamedTensor(
                               "y", TensorFrom(arrow::float32(), "[4.12]"))},
            ReduceTestCase{
                .op_type = ReduceMin::kOpType,
                .status = pb::TENSORSTATUS_SECRET,
                .input = test::NamedTensor(
                    "x", TensorFrom(arrow::int64(),
                                    "[1000, -2, 3345, 42, 5999, 60]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::int64(),
                                                            "[-2]"))},
            ReduceTestCase{.op_type = ReduceMin::kOpType,
                           .status = pb::TENSORSTATUS_SECRET,
                           .input = test::NamedTensor(
                               "x", TensorFrom(arrow::float32(),
                                               "[1.75, 2.34, 4.12, 1.99]")),
                           .output = test::NamedTensor(
                               "y", TensorFrom(arrow::float32(), "[1.75]"))},
            // testcase: empty inputs
            ReduceTestCase{
                .op_type = ReduceSum::kOpType,
                .status = pb::TENSORSTATUS_SECRET,
                .input = test::NamedTensor("x",
                                           TensorFrom(arrow::float32(), "[]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float32(),
                                                            "[]"))},
            ReduceTestCase{
                .op_type = ReduceAvg::kOpType,
                .status = pb::TENSORSTATUS_SECRET,
                .input = test::NamedTensor("x",
                                           TensorFrom(arrow::int64(), "[]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float64(),
                                                            "[]"))},
            ReduceTestCase{
                .op_type = ReduceMin::kOpType,
                .status = pb::TENSORSTATUS_SECRET,
                .input = test::NamedTensor("x",
                                           TensorFrom(arrow::float32(), "[]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float32(),
                                                            "[]"))},
            ReduceTestCase{
                .op_type = ReduceMax::kOpType,
                .status = pb::TENSORSTATUS_SECRET,
                .input = test::NamedTensor("x",
                                           TensorFrom(arrow::float32(), "[]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float32(),
                                                            "[]"))})),
    TestParamNameGenerator(ReduceTest));

TEST_P(ReduceTest, Works) {
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
  auto op_creator = [&]() {
    return GetOpRegistry()->GetOperator(node.op_type());
  };

  if (tc.status == pb::TENSORSTATUS_PRIVATE) {
    EXPECT_NO_THROW(test::RunOpAsync({ctx_ptrs[0]}, op_creator));
  } else {
    EXPECT_NO_THROW(test::RunOpAsync(ctx_ptrs, op_creator));
  }

  TensorPtr actual_output = nullptr;
  if (tc.status == pb::TENSORSTATUS_PRIVATE) {
    actual_output = ctx_ptrs[0]->GetTensorTable()->GetTensor(tc.output.name);
  } else {
    EXPECT_NO_THROW(
        { actual_output = test::RevealSecret(ctx_ptrs, tc.output.name); });
  }
  ASSERT_TRUE(actual_output != nullptr);
  auto actual_arr = actual_output->ToArrowChunkedArray();
  auto expect_arr = tc.output.tensor->ToArrowChunkedArray();
  EXPECT_TRUE(actual_arr->ApproxEquals(
      *expect_arr, arrow::EqualOptions::Defaults().atol(0.001)))
      << "expect type = " << expect_arr->type()->ToString()
      << ", got type = " << actual_arr->type()->ToString()
      << "\nexpect result = " << expect_arr->ToString()
      << "\nbut actual got result = " << actual_arr->ToString();
}

pb::ExecNode ReduceTest::MakeExecNode(const ReduceTestCase& tc) {
  test::ExecNodeBuilder builder(tc.op_type);

  builder.SetNodeName(tc.op_type + "-test");
  auto input = test::MakeTensorReference(tc.input.name, tc.input.tensor->Type(),
                                         tc.status);
  builder.AddInput(ReduceBase::kIn, {input});
  auto output = test::MakeTensorReference(tc.output.name,
                                          tc.output.tensor->Type(), tc.status);
  builder.AddOutput(ReduceBase::kOut, {output});

  return builder.Build();
}

void ReduceTest::FeedInputs(const std::vector<ExecContext*>& ctxs,
                            const ReduceTestCase& tc) {
  if (tc.status == pb::TENSORSTATUS_PRIVATE) {
    test::FeedInputsAsPrivate(ctxs[0], {tc.input});
  } else {
    test::FeedInputsAsSecret(ctxs, {tc.input});
  }
}

}  // namespace scql::engine::op
