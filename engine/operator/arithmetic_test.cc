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

#include "engine/operator/arithmetic.h"

#include "gtest/gtest.h"

#include "engine/operator/binary_test.h"

namespace scql::engine::op {

// TODO: add more test values
INSTANTIATE_TEST_SUITE_P(
    ArithmeticBatchTest, BinaryComputeInSecretTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            BinaryTestCase{
                .op_type = Add::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[1,2,3]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[1,2,3]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::int64(),
                                                              "[2,4,6]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            BinaryTestCase{
                .op_type = Minus::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[100,123,999]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[-1,100,1000]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::int64(),
                                                              "[101,23,-1]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            BinaryTestCase{
                .op_type = Mul::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[100,123,12]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[-1,100,12]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor(
                    "z", TensorFrom(arrow::int64(), "[-100,12300,144]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            BinaryTestCase{
                .op_type = Div::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[100,9]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[3,3]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::float64(),
                                                              "[33.33,3.0]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            BinaryTestCase{
                .op_type = IntDiv::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[100,9,5,5,3]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[3,3,2,3,2]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::int64(),
                                                              "[33,3,2,1,1]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            BinaryTestCase{
                .op_type = Mod::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(),
                                    "[100,9,5,-100,-9,-5,100,9,5,-100,-9,-"
                                    "5]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(),
                                    "[33,3,5,33,3,5,-33,-3,-5,-33,-3,-5]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor(
                    "z",
                    TensorFrom(arrow::int64(), "[1,0,0,-1,0,0,1,0,0,-1,0,0]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            // testcase with empty inputs
            BinaryTestCase{
                .op_type = Add::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::int64(),
                                                              "[]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            BinaryTestCase{
                .op_type = Minus::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::int64(),
                                                              "[]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            BinaryTestCase{
                .op_type = Mul::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::int64(),
                                                              "[]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            BinaryTestCase{
                .op_type = Div::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::float64(),
                                                              "[]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            BinaryTestCase{
                .op_type = IntDiv::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::int64(),
                                                              "[]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            },
            BinaryTestCase{
                .op_type = Mod::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[]"))},
                .left_input_status = pb::TENSORSTATUS_SECRET,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[]"))},
                .right_input_status = pb::TENSORSTATUS_SECRET,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::int64(),
                                                              "[]"))},
                .output_status = pb::TENSORSTATUS_SECRET,
            })),
    TestParamNameGenerator(BinaryComputeInSecretTest));

INSTANTIATE_TEST_SUITE_P(
    ArithmeticBatchTest, BinaryComputeInPlainTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            BinaryTestCase{
                .op_type = Add::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[1,2,3]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[1,null,3]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor("z", TensorFrom(arrow::int64(),
                                                              "[2,null,6]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            },
            BinaryTestCase{
                .op_type = Minus::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[100,123,999,4,null]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[-1,100,1000,null,5]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor(
                    "z", TensorFrom(arrow::int64(), "[101,23,-1,null,null]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            },
            BinaryTestCase{
                .op_type = Mul::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[100,123,12,4,null]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[-1,100,12,null,5]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor(
                    "z",
                    TensorFrom(arrow::int64(), "[-100,12300,144,null,null]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            },
            BinaryTestCase{
                .op_type = Div::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::float64(), "[100.0,9,4,null]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[3,3,null,5]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor(
                    "z",
                    TensorFrom(arrow::float64(), "[33.33333333,3,null,null]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            },
            BinaryTestCase{
                .op_type = IntDiv::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(), "[100,9,5,4,null]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::int64(), "[3,3,2,null,5]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor(
                    "z", TensorFrom(arrow::int64(), "[33,3,2,null,null]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            },
            BinaryTestCase{
                .op_type = Mod::kOpType,
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int64(),
                                    "[100,9,5,-100,-9,-5,100,9,5,-100,-9,-"
                                    "5,null,1,null]"))},
                .left_input_status = pb::TENSORSTATUS_PRIVATE,
                .right_inputs = {test::NamedTensor(
                    "y",
                    TensorFrom(
                        arrow::int64(),
                        "[33,3,5,33,3,5,-33,-3,-5,-33,-3,-5,1,null,null]"))},
                .right_input_status = pb::TENSORSTATUS_PRIVATE,
                .outputs = {test::NamedTensor(
                    "z",
                    TensorFrom(arrow::int64(),
                               "[1,0,0,-1,0,0,1,0,0,-1,0,0,null,null,null]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE,
            })),
    TestParamNameGenerator(BinaryComputeInPlainTest));

}  // namespace scql::engine::op