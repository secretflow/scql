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