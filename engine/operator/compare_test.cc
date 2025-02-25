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

#include "engine/operator/compare.h"

#include "gtest/gtest.h"

#include "engine/operator/binary_test.h"

namespace scql::engine::op {

struct VariadicCompareTestCase {
  std::string op_type;
  std::vector<test::NamedTensor> inputs;
  pb::TensorStatus input_status;

  std::vector<test::NamedTensor> outputs;
  pb::TensorStatus output_status;
};

class VariadicCompareTest
    : public testing::TestWithParam<
          std::tuple<test::SpuRuntimeTestCase, VariadicCompareTestCase>> {
 protected:
  void SetUp() override { RegisterAllOps(); }

 public:
  static pb::ExecNode MakeExecNode(const VariadicCompareTestCase& tc);
  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const VariadicCompareTestCase& tc);

  static std::unique_ptr<Operator> CreateOp(const std::string& op_type) {
    return GetOpRegistry()->GetOperator(op_type);
  }
};

pb::ExecNode VariadicCompareTest::MakeExecNode(
    const VariadicCompareTestCase& tc) {
  test::ExecNodeBuilder builder(tc.op_type);
  builder.SetNodeName(tc.op_type + "-test");
  std::vector<pb::Tensor> inputs;
  std::vector<pb::Tensor> outputs;
  for (const auto& tensor : tc.inputs) {
    auto t = test::MakeTensorReference(tensor.name, tensor.tensor->Type(),
                                       tc.input_status);
    inputs.push_back(t);
  }

  {
    auto t = test::MakeTensorReference(
        tc.outputs[0].name, tc.outputs[0].tensor->Type(), tc.output_status);
    outputs.push_back(t);
  }

  builder.AddInput(Variadic::kIn, inputs);
  builder.AddOutput(Variadic::kOut, outputs);

  return builder.Build();
}

void VariadicCompareTest::FeedInputs(const std::vector<ExecContext*>& ctxs,
                                     const VariadicCompareTestCase& tc) {
  auto infeed = [](const std::vector<ExecContext*>& ctxs,
                   pb::TensorStatus input_status,
                   const std::vector<test::NamedTensor>& inputs) {
    if (input_status == pb::TENSORSTATUS_PRIVATE) {
      // feed private inputs into ctxs[0]
      test::FeedInputsAsPrivate(ctxs[0], inputs);
    } else if (input_status == pb::TENSORSTATUS_SECRET) {
      test::FeedInputsAsSecret(ctxs, inputs);
    } else {
      test::FeedInputsAsPublic(ctxs, inputs);
    }
  };

  infeed(ctxs, tc.input_status, tc.inputs);
}

// ===========================
//   Greatest test suit
// ===========================
INSTANTIATE_TEST_SUITE_P(
    VariadicCompareTests, VariadicCompareTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            VariadicCompareTestCase{
                .op_type = Greatest::kOpType,
                .inputs = {test::NamedTensor("x", TensorFrom(arrow::int64(),
                                                             "[1, 2, 3]")),
                           test::NamedTensor("y", TensorFrom(arrow::int64(),
                                                             "[0, 2, 4]"))},
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::int64(),
                                                              "[1, 2, 4]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            },
            VariadicCompareTestCase{
                .op_type = Greatest::kOpType,
                .inputs = {test::NamedTensor("x", TensorFrom(arrow::int64(),
                                                             "[1, 2, 3]")),
                           test::NamedTensor("y", TensorFrom(arrow::int64(),
                                                             "[0, 2, 4]"))},
                .input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::int64(),
                                                              "[1, 2, 4]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            VariadicCompareTestCase{
                .op_type = Least::kOpType,
                .inputs = {test::NamedTensor("x", TensorFrom(arrow::int64(),
                                                             "[1, 2, 3]")),
                           test::NamedTensor("y", TensorFrom(arrow::int64(),
                                                             "[0, 2, 4]"))},
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::int64(),
                                                              "[0, 2, 3]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            },
            VariadicCompareTestCase{
                .op_type = Least::kOpType,
                .inputs = {test::NamedTensor("x", TensorFrom(arrow::int64(),
                                                             "[1, 2, 3]")),
                           test::NamedTensor("y", TensorFrom(arrow::int64(),
                                                             "[0, 2, 4]"))},
                .input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::int64(),
                                                              "[0, 2, 3]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            })));

// ===========================
//   Equal test suit
// ===========================

INSTANTIATE_TEST_SUITE_P(
    EqualBatchTest, BinaryComputeInSecretTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            BinaryTestCase{
                .op_type = Equal::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[1, 2, 3]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[0, 2, 4]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[0, 1, 0]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            BinaryTestCase{
                .op_type = Equal::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::float32(),
                                    "[-100.0, -3.1415, 2.0, 3.14]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::float32(),
                                    "[-200, -3.1415, 2.01, 3.1415]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[0, 1, 0, 0]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            BinaryTestCase{
                .op_type = Equal::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(),
                                    R"json(["A", "B", "C", "D"])json"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["A", "C", "B", "D"])json"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[1, 0, 0, 1]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            // testcase with empty inputs
            BinaryTestCase{
                .op_type = Equal::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            })),
    TestParamNameGenerator(BinaryComputeInSecretTest));

INSTANTIATE_TEST_SUITE_P(
    EqualBatchTest, BinaryComputeInPlainTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            BinaryTestCase{
                .op_type = Equal::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[1, 2, 3]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[0, null, 3]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[0, null, 1]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            },
            BinaryTestCase{
                .op_type = Equal::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::float32(),
                                    "[-100.0, -3.1415, null, 3.14, 100]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::float32(),
                                    "[-200, -3.1415, 1.999, 3.1415, 99.9]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor(
                    "z", TensorFrom(arrow::boolean(), "[0, 1, null, 0, 0]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            },
            BinaryTestCase{
                .op_type = Equal::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(),
                                    R"json(["A", "B", "C", "D"])json"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["A", "C", "B", "D"])json"))},
                .right_input_status = pb::TENSORSTATUS_PUBLIC,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[1, 0, 0, 1]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            },
            BinaryTestCase{
                .op_type = Equal::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::float32(), "[]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::float32(), "[]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            })),
    TestParamNameGenerator(BinaryComputeInSecretTest));

// ===========================
//   NotEqual test suit
// ===========================

INSTANTIATE_TEST_SUITE_P(
    NotEqualBatchTest, BinaryComputeInSecretTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            BinaryTestCase{
                .op_type = NotEqual::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[1, 2, 3]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[0, 2, 4]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[1, 0, 1]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            BinaryTestCase{
                .op_type = NotEqual::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::float32(),
                                    "[-100.0, -3.1415, 2.0, 3.14]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::float32(),
                                    "[-200, -3.1415, 2.01, 3.1415]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[1, 0, 1, 1]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            BinaryTestCase{
                .op_type = NotEqual::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(),
                                    R"json(["A", "B", "C", "D"])json"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["A", "C", "B", "D"])json"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[0, 1, 1, 0]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            // testcase with empty inputs
            BinaryTestCase{
                .op_type = NotEqual::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            })),
    TestParamNameGenerator(BinaryComputeInSecretTest));

INSTANTIATE_TEST_SUITE_P(
    NotEqualBatchTest, BinaryComputeInPlainTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            BinaryTestCase{
                .op_type = NotEqual::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[1, 2, 3]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[0, null, 3]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[1, null, 0]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            },
            BinaryTestCase{
                .op_type = NotEqual::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::float32(),
                                    "[-100.0, -3.1415, null, 3.14, 100]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::float32(),
                                    "[-200, -3.1415, 1.999, 3.1415, 99.9]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor(
                    "z", TensorFrom(arrow::boolean(), "[1, 0, null, 1, 1]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            },
            BinaryTestCase{
                .op_type = NotEqual::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(),
                                    R"json(["A", "B", "C", "D"])json"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["A", "C", "B", "D"])json"))},
                .right_input_status = pb::TENSORSTATUS_PUBLIC,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[0, 1, 1, 0]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            })),
    TestParamNameGenerator(BinaryComputeInPlainTest));

// ===========================
//   Less test suit
// ===========================

INSTANTIATE_TEST_SUITE_P(
    LessBatchTest, BinaryComputeInSecretTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            BinaryTestCase{
                .op_type = Less::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[1, 2, 3]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[0, 2, 4]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[0, 0, 1]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            BinaryTestCase{
                .op_type = Less::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::float32(),
                                    "[-100.0, -3.1415, 2.0, 3.14]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::float32(),
                                    "[-200, -3.14, 1.999, 3.1415]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[0, 1, 0, 1]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            // testcase: with empty inputs
            BinaryTestCase{
                .op_type = Less::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            })),
    TestParamNameGenerator(BinaryComputeInSecretTest));

INSTANTIATE_TEST_SUITE_P(
    LessBatchTest, BinaryComputeInPlainTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            BinaryTestCase{
                .op_type = Less::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[1, 2, 3]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[0, null, 4]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[0, null, 1]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            },
            BinaryTestCase{
                .op_type = Less::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::float32(),
                                    "[-100.0, -3.1415, null, 3.14, 100]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::float32(),
                                    "[-200, -3.14, 1.999, 3.1415, 99.9]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor(
                    "z", TensorFrom(arrow::boolean(), "[0, 1, null, 1, 0]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            })),
    TestParamNameGenerator(BinaryComputeInPlainTest));

// ===========================
//   LessEqual test suit
// ===========================

INSTANTIATE_TEST_SUITE_P(
    LessEqualBatchTest, BinaryComputeInSecretTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            BinaryTestCase{
                .op_type = LessEqual::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[1, 2, 3]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[0, 2, 4]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[0, 1, 1]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            BinaryTestCase{
                .op_type = LessEqual::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::float32(),
                                    "[-100.0, -3.1415, 2.0, 3.14]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::float32(),
                                    "[-200, -3.14, 1.999, 3.1415]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[0, 1, 0, 1]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            // testcase with empty inputs
            BinaryTestCase{
                .op_type = LessEqual::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            })),
    TestParamNameGenerator(BinaryComputeInSecretTest));

INSTANTIATE_TEST_SUITE_P(
    LessEqualBatchTest, BinaryComputeInPlainTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            BinaryTestCase{
                .op_type = LessEqual::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[1, 2, 3]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[0, null, 4]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[0, null, 1]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            },
            BinaryTestCase{
                .op_type = LessEqual::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::float32(),
                                    "[-100.0, -3.1415, null, 3.14, 100]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::float32(),
                                    "[-200, -3.14, 1.999, 3.1415, 99.9]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor(
                    "z", TensorFrom(arrow::boolean(), "[0, 1, null, 1, 0]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            })),
    TestParamNameGenerator(BinaryComputeInPlainTest));

// ===========================
//   GreaterEqual test suit
// ===========================

INSTANTIATE_TEST_SUITE_P(
    GreaterEqualBatchTest, BinaryComputeInSecretTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            BinaryTestCase{
                .op_type = GreaterEqual::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[1, 2, 3]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[0, 2, 4]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[1, 1, 0]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            BinaryTestCase{
                .op_type = GreaterEqual::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::float32(),
                                    "[-100.0, -3.1415, 2.0, 3.14]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::float32(),
                                    "[-200, -3.14, 1.999, 3.1415]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[1, 0, 1, 0]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            // testcase with empty inputs
            BinaryTestCase{
                .op_type = GreaterEqual::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            })),
    TestParamNameGenerator(BinaryComputeInSecretTest));

INSTANTIATE_TEST_SUITE_P(
    GreaterEqualBatchTest, BinaryComputeInPlainTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            BinaryTestCase{
                .op_type = GreaterEqual::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[1, 2, 3]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[0, null, 4]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[1, null, 0]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            },
            BinaryTestCase{
                .op_type = GreaterEqual::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::float32(),
                                    "[-100.0, -3.1415, null, 3.14, 100]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::float32(),
                                    "[-200, -3.14, 1.999, 3.1415, 99.9]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor(
                    "z", TensorFrom(arrow::boolean(), "[1, 0, null, 0, 1]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            })),
    TestParamNameGenerator(BinaryComputeInPlainTest));

// ===========================
//   Greater test suit
// ===========================

INSTANTIATE_TEST_SUITE_P(
    GreaterBatchTest, BinaryComputeInSecretTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            BinaryTestCase{
                .op_type = Greater::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[1, 2, 3]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[0, 2, 4]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[1, 0, 0]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            BinaryTestCase{
                .op_type = Greater::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::float32(),
                                    "[-100.0, -3.1415, 2.0, 3.14]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::float32(),
                                    "[-200, -3.14, 1.999, 3.1415]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[1, 0, 1, 0]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            // testcase with empty inputs
            BinaryTestCase{
                .op_type = Greater::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            })),
    TestParamNameGenerator(BinaryComputeInSecretTest));

TEST_P(VariadicCompareTest, Works) {
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
  FeedInputs(ctx_ptrs, tc);

  auto op = CreateOp(tc.op_type);
  if (tc.output_status == pb::TENSORSTATUS_PRIVATE) {
    EXPECT_NO_THROW(op->Run(ctx_ptrs[0]));
  } else {
    EXPECT_NO_THROW(
        test::RunOpAsync(ctx_ptrs, [&]() { return CreateOp(tc.op_type); }));
  }

  for (const auto& named_tensor : tc.outputs) {
    TensorPtr t = nullptr;
    if (tc.output_status == pb::TENSORSTATUS_PRIVATE) {
      EXPECT_NO_THROW(
          t = ctx_ptrs[0]->GetTensorTable()->GetTensor(named_tensor.name));
    } else {
      EXPECT_NO_THROW({ t = test::RevealSecret(ctx_ptrs, named_tensor.name); });
    }

    ASSERT_TRUE(t != nullptr);
    EXPECT_TRUE(t->ToArrowChunkedArray()->ApproxEquals(
        *named_tensor.tensor->ToArrowChunkedArray(),
        arrow::EqualOptions::Defaults().atol(0.05)))
        << "expect type = "
        << named_tensor.tensor->ToArrowChunkedArray()->type()->ToString()
        << ", got type = " << t->ToArrowChunkedArray()->type()->ToString()
        << "\nexpect result = "
        << named_tensor.tensor->ToArrowChunkedArray()->ToString()
        << "\nbut actual got result = " << t->ToArrowChunkedArray()->ToString();
  }
}

INSTANTIATE_TEST_SUITE_P(
    GreaterBatchTest, BinaryComputeInPlainTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            BinaryTestCase{
                .op_type = Greater::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[1, 2, 3]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[0, null, 4]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                              "[1, null, 0]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            },
            BinaryTestCase{
                .op_type = Greater::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::float32(),
                                    "[-100.0, -3.1415, null, 3.14, 100]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::float32(),
                                    "[-200, -3.14, 1.999, 3.1415, 99.9]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor(
                    "z", TensorFrom(arrow::boolean(), "[1, 0, null, 0, 1]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            })),
    TestParamNameGenerator(BinaryComputeInPlainTest));

}  // namespace scql::engine::op