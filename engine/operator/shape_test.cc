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

#include "engine/operator/shape.h"

#include "arrow/type.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct ShapeTestCase {
  std::vector<test::NamedTensor> inputs;
  pb::TensorStatus input_status;
  int64_t axis;
  std::vector<test::NamedTensor> expect_outs;
};

class ShapeTest : public testing::TestWithParam<
                      std::tuple<test::SpuRuntimeTestCase, ShapeTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const ShapeTestCase& tc);
  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const ShapeTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    ShapeBatchTest, ShapeTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            ShapeTestCase{
                .inputs =
                    {test::NamedTensor("a", TensorFrom(arrow::boolean(),
                                                       "[true, false, true]")),
                     test::NamedTensor("b", TensorFrom(arrow::int64(),
                                                       "[1,2,3,4,5,6,7,null]")),
                     test::NamedTensor(
                         "c", TensorFrom(
                                  arrow::float32(),
                                  "[1.1025, 100.245, -10.2, 0.34, 3.1415926]")),
                     test::NamedTensor(
                         "d", TensorFrom(arrow::large_utf8(),
                                         R"json(["A","B","C","D"])json")),
                     test::NamedTensor("e", TensorFrom(arrow::int64(), "[]"))},
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .axis = Shape::kAxisDefault,
                .expect_outs =
                    {test::NamedTensor("a_out",
                                       TensorFrom(arrow::int64(), "[3,1]")),
                     test::NamedTensor("b_out",
                                       TensorFrom(arrow::int64(), "[8,1]")),
                     test::NamedTensor("c_out",
                                       TensorFrom(arrow::int64(), "[5,1]")),
                     test::NamedTensor("d_out",
                                       TensorFrom(arrow::int64(), "[4,1]")),
                     test::NamedTensor("e_out",
                                       TensorFrom(arrow::int64(), "[0,1]"))}},
            ShapeTestCase{
                .inputs =
                    {test::NamedTensor("a", TensorFrom(arrow::boolean(),
                                                       "[true, false, true]")),
                     test::NamedTensor("b", TensorFrom(arrow::int64(),
                                                       "[1,2,3,4,5,6,7,8]")),
                     test::NamedTensor(
                         "c", TensorFrom(
                                  arrow::float32(),
                                  "[1.1025, 100.245, -10.2, 0.34, 3.1415926]")),
                     test::NamedTensor("d", TensorFrom(arrow::int64(), "[]"))},
                .input_status = pb::TENSORSTATUS_SECRET,
                .axis = Shape::kAxisRow,
                .expect_outs =
                    {test::NamedTensor("a_out",
                                       TensorFrom(arrow::int64(), "[3]")),
                     test::NamedTensor("b_out",
                                       TensorFrom(arrow::int64(), "[8]")),
                     test::NamedTensor("c_out",
                                       TensorFrom(arrow::int64(), "[5]")),
                     test::NamedTensor("d_out",
                                       TensorFrom(arrow::int64(), "[0]"))}})),
    TestParamNameGenerator(ShapeTest));

TEST_P(ShapeTest, works) {
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
  Shape op;
  EXPECT_NO_THROW(op.Run(ctx_ptrs[0]));

  // Then
  // check alice output
  for (const auto& expect_t : tc.expect_outs) {
    auto expect_arr = expect_t.tensor->ToArrowChunkedArray();
    auto out = ctx_ptrs[0]->GetTensorTable()->GetTensor(expect_t.name);
    ASSERT_TRUE(out);
    auto out_arr = out->ToArrowChunkedArray();
    // compare tensor content
    EXPECT_TRUE(out_arr->ApproxEquals(*expect_arr))
        << "expect type = " << expect_arr->type()->ToString()
        << ", got type = " << out_arr->type()->ToString()
        << "\nexpect result = " << expect_arr->ToString()
        << "\nbut actual got result = " << out_arr->ToString();
  }
}

/// ===================
/// ShapeTest impl
/// ===================

pb::ExecNode ShapeTest::MakeExecNode(const ShapeTestCase& tc) {
  test::ExecNodeBuilder builder(Shape::kOpType);

  builder.SetNodeName("shape-test");
  builder.AddInt64Attr(Shape::kAxis, tc.axis);

  std::vector<pb::Tensor> inputs;
  for (const auto& named_tensor : tc.inputs) {
    auto t = test::MakeTensorReference(
        named_tensor.name, named_tensor.tensor->Type(), tc.input_status);
    inputs.push_back(std::move(t));
  }
  builder.AddInput(Shape::kIn, inputs);

  std::vector<pb::Tensor> outputs;
  for (size_t i = 0; i < tc.expect_outs.size(); ++i) {
    auto t = test::MakePrivateTensorReference(tc.expect_outs[i].name,
                                              pb::PrimitiveDataType::INT64);
    outputs.push_back(std::move(t));
  }
  builder.AddOutput(Shape::kOut, outputs);

  return builder.Build();
}

void ShapeTest::FeedInputs(const std::vector<ExecContext*>& ctxs,
                           const ShapeTestCase& tc) {
  if (tc.input_status == pb::TENSORSTATUS_PRIVATE) {
    test::FeedInputsAsPrivate(ctxs[0], tc.inputs);
  } else {
    test::FeedInputsAsSecret(ctxs, tc.inputs);
  }
}

}  // namespace scql::engine::op