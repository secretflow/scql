// Copyright 2024 Ant Group Co., Ltd.
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

#include "unary.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/framework/registry.h"
#include "engine/operator/all_ops_register.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {
struct UnaryTestCase {
  std::string op;
  bool correct;
  test::NamedTensor input;
  test::NamedTensor output;
  pb::TensorStatus input_status;
  pb::TensorStatus output_status;
  size_t world_size = 1;
};

class UnaryTest : public ::testing::TestWithParam<
                      std::tuple<test::SpuRuntimeTestCase, UnaryTestCase>> {
 protected:
  void SetUp() override { RegisterAllOps(); };
  static pb::ExecNode MakeExecNode(const UnaryTestCase& tc,
                                   const std::string& name);
  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const UnaryTestCase& tc);
};

pb::ExecNode UnaryTest::MakeExecNode(const UnaryTestCase& tc,
                                     const std::string& name) {
  test::ExecNodeBuilder builder(name);
  builder.SetNodeName(name + "-test");

  {
    auto input = test::MakeTensorReference(
        tc.input.name, tc.input.tensor->Type(), tc.input_status);
    builder.AddInput(UnaryBase::kIn, {input});
  }

  {
    auto output = test::MakeTensorReference(
        tc.output.name, tc.output.tensor->Type(), tc.output_status);
    builder.AddOutput(UnaryBase::kOut, {output});
  }

  return builder.Build();
}

void UnaryTest::FeedInputs(const std::vector<ExecContext*>& ctxs,
                           const UnaryTestCase& tc) {
  if (tc.input_status == pb::TensorStatus::TENSORSTATUS_PRIVATE) {
    for (auto* ctx : ctxs) {
      test::FeedInputsAsPrivate(ctx, {tc.input});
    }
  } else if (tc.input_status == pb::TensorStatus::TENSORSTATUS_SECRET) {
    test::FeedInputsAsSecret(ctxs, {tc.input});
  } else {
    test::FeedInputsAsPublic(ctxs, {tc.input});
  }
}

INSTANTIATE_TEST_SUITE_P(
    UnaryTestSuite, UnaryTest,
    testing::Combine(
        test::SpuTestValues2PC,
        testing::Values(
            UnaryTestCase{
                .op = Abs::kOpType,
                .correct = true,
                .input = test::NamedTensor("x", TensorFrom(arrow::float64(),
                                                           "[-1, 0, 1]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float64(),
                                                            "[1, 0, 1]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output_status = pb::TENSORSTATUS_PRIVATE},
            UnaryTestCase{
                .op = Abs::kOpType,
                .correct = true,
                .input = test::NamedTensor("x", TensorFrom(arrow::float64(),
                                                           "[-1.5, 0, 1]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float64(),
                                                            "[1.5, 0, 1]")),
                .input_status = pb::TENSORSTATUS_SECRET,
                .output_status = pb::TENSORSTATUS_SECRET,
                .world_size = 2},
            UnaryTestCase{
                .op = Ceil::kOpType,
                .correct = true,
                .input = test::NamedTensor("x", TensorFrom(arrow::float64(),
                                                           "[-1.5, 0.6, 1.1]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float64(),
                                                            "[-1, 1, 2]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output_status = pb::TENSORSTATUS_PRIVATE},
            UnaryTestCase{
                .op = Ceil::kOpType,
                .correct = true,
                .input = test::NamedTensor("x", TensorFrom(arrow::float64(),
                                                           "[-1.5, 0.6, 1.1]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float64(),
                                                            "[-1, 1, 2]")),
                .input_status = pb::TENSORSTATUS_SECRET,
                .output_status = pb::TENSORSTATUS_SECRET,
                .world_size = 2},
            UnaryTestCase{
                .op = Floor::kOpType,
                .correct = true,
                .input = test::NamedTensor("x", TensorFrom(arrow::float64(),
                                                           "[-1.5, 0.1, 1.9]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float64(),
                                                            "[-2, 0, 1]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output_status = pb::TENSORSTATUS_PRIVATE},
            UnaryTestCase{
                .op = Floor::kOpType,
                .correct = true,
                .input = test::NamedTensor("x", TensorFrom(arrow::float64(),
                                                           "[-1.5, 0.1, 1.9]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float64(),
                                                            "[-2, 0, 1]")),
                .input_status = pb::TENSORSTATUS_SECRET,
                .output_status = pb::TENSORSTATUS_SECRET,
                .world_size = 2},
            UnaryTestCase{
                .op = Round::kOpType,
                .correct = true,
                .input = test::NamedTensor(
                    "x",
                    TensorFrom(arrow::float64(), "[-1.5, -1.1, 0.1, 1.9]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float64(),
                                                            "[-2, -1, 0, 2]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output_status = pb::TENSORSTATUS_PRIVATE},
            UnaryTestCase{
                .op = Radians::kOpType,
                .correct = true,
                .input = test::NamedTensor("x", TensorFrom(arrow::float64(),
                                                           "[0, 180, 360]")),
                .output = test::NamedTensor(
                    "y", TensorFrom(arrow::float64(),
                                    "[0, 3.1415926, 6.28318530718]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output_status = pb::TENSORSTATUS_PRIVATE},
            UnaryTestCase{
                .op = Radians::kOpType,
                .correct = true,
                .input = test::NamedTensor("x", TensorFrom(arrow::float64(),
                                                           "[0, 180, 360]")),
                .output = test::NamedTensor(
                    "y", TensorFrom(arrow::float64(),
                                    "[0, 3.1415926, 6.28318530718]")),
                .input_status = pb::TENSORSTATUS_SECRET,
                .output_status = pb::TENSORSTATUS_SECRET,
                .world_size = 2},
            UnaryTestCase{
                .op = Degrees::kOpType,
                .correct = true,
                .input = test::NamedTensor(
                    "x", TensorFrom(arrow::float64(),
                                    "[0, 3.1415926, 6.28318530718]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float64(),
                                                            "[0, 180, 360]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output_status = pb::TENSORSTATUS_PRIVATE},
            UnaryTestCase{
                .op = Degrees::kOpType,
                .correct = true,
                .input = test::NamedTensor(
                    "x", TensorFrom(arrow::float64(),
                                    "[0, 3.1415926, 6.28318530718]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float64(),
                                                            "[0, 180, 360]")),
                .input_status = pb::TENSORSTATUS_SECRET,
                .output_status = pb::TENSORSTATUS_SECRET,
                .world_size = 2},
            UnaryTestCase{
                .op = Ln::kOpType,
                .correct = true,
                .input = test::NamedTensor(
                    "x", TensorFrom(arrow::float64(), "[1, 2.71828182846]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float64(),
                                                            "[0, 1]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output_status = pb::TENSORSTATUS_PRIVATE},
            UnaryTestCase{
                .op = Ln::kOpType,
                .correct = true,
                .input = test::NamedTensor(
                    "x", TensorFrom(arrow::float64(), "[1, 2.71828182846]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float64(),
                                                            "[0, 1]")),
                .input_status = pb::TENSORSTATUS_SECRET,
                .output_status = pb::TENSORSTATUS_SECRET,
                .world_size = 2},
            UnaryTestCase{
                .op = Log10::kOpType,
                .correct = true,
                .input = test::NamedTensor("x", TensorFrom(arrow::float64(),
                                                           "[1, 10, 100]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float64(),
                                                            "[0, 1, 2]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output_status = pb::TENSORSTATUS_PRIVATE},
            UnaryTestCase{
                .op = Log2::kOpType,
                .correct = true,
                .input = test::NamedTensor("x", TensorFrom(arrow::float64(),
                                                           "[1, 2, 4, 8]")),
                .output = test::NamedTensor("y", TensorFrom(arrow::float64(),
                                                            "[0, 1, 2, 3]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output_status = pb::TENSORSTATUS_PRIVATE},
            UnaryTestCase{
                .op = Sqrt::kOpType,
                .correct = true,
                .input = test::NamedTensor("x", TensorFrom(arrow::float64(),
                                                           "[0, 1, 2, 4, 9]")),
                .output = test::NamedTensor(
                    "y", TensorFrom(arrow::float64(),
                                    "[0, 1, 1.41421356237, 2, 3]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output_status = pb::TENSORSTATUS_PRIVATE},
            UnaryTestCase{
                .op = Sqrt::kOpType,
                .correct = true,
                .input = test::NamedTensor("x", TensorFrom(arrow::float64(),
                                                           "[0, 1, 2, 4, 9]")),
                .output = test::NamedTensor(
                    "y", TensorFrom(arrow::float64(),
                                    "[0, 1, 1.41421356237, 2, 3]")),
                .input_status = pb::TENSORSTATUS_SECRET,
                .output_status = pb::TENSORSTATUS_SECRET,
                .world_size = 2},
            UnaryTestCase{
                .op = Exp::kOpType,
                .correct = true,
                .input = test::NamedTensor("x", TensorFrom(arrow::float64(),
                                                           "[0, 1, 2, 3]")),
                .output = test::NamedTensor(
                    "y",
                    TensorFrom(
                        arrow::float64(),
                        "[1, 2.718281828, 7.389056098931, 20.085536923188]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output_status = pb::TENSORSTATUS_PRIVATE},
            UnaryTestCase{
                .op = Exp::kOpType,
                .correct = true,
                .input = test::NamedTensor("x", TensorFrom(arrow::float64(),
                                                           "[0, 1, 2]")),
                .output = test::NamedTensor(
                    "y",
                    TensorFrom(arrow::float64(),
                               "[1, 2.7129974365234375, 7.332767486572266]")),
                .input_status = pb::TENSORSTATUS_SECRET,
                .output_status = pb::TENSORSTATUS_SECRET,
                .world_size = 2})),
    TestParamNameGenerator(UnaryTest));

TEST_P(UnaryTest, WorksCorrectly) {
  auto param = GetParam();
  auto tc = std::get<1>(param);
  auto node = MakeExecNode(tc, tc.op);
  std::vector<std::shared_ptr<scql::engine::Session>> sessions;
  double tolerance = 0.001;
  if (tc.world_size > 1) {
    auto spu_tc = std::get<0>(param);
    if (tc.op == Exp::kOpType) {
      // The exp operation tends to be less accurate, particularly when using
      // the CHEETAH protocol.
      if (spu_tc.protocol == spu::ProtocolKind::CHEETAH) {
        tolerance = 0.05;
      } else {
        tolerance = 0.005;
      }
    }

    sessions = test::MakeMultiPCSession(spu_tc);
  } else {
    sessions.push_back(test::Make1PCSession());
  }

  std::vector<ExecContext*> ctxs;
  std::vector<ExecContext> execs;

  execs.reserve(sessions.size());
  for (auto& session : sessions) {
    execs.push_back(ExecContext(node, session.get()));
  }

  ctxs.reserve(execs.size());
  for (auto& exec : execs) {
    ctxs.push_back(&exec);
  }
  FeedInputs(ctxs, tc);

  auto func = [&]() { return GetOpRegistry()->GetOperator(tc.op); };

  if (tc.correct) {
    test::RunOpAsync(ctxs, func);

    // EXPECT_NO_THROW(op_handlers[tc.op](ctxs));
    TensorPtr result;
    if (tc.input_status == pb::TENSORSTATUS_PRIVATE) {
      result = ctxs[0]->GetTensorTable()->GetTensor(tc.output.name);
    } else {
      EXPECT_NO_THROW(result = test::RevealSecret(ctxs, tc.output.name));
    }

    ASSERT_TRUE(result);
    auto out_arr = result->ToArrowChunkedArray();
    auto expect_arr = tc.output.tensor->ToArrowChunkedArray();

    EXPECT_TRUE(out_arr->length() == expect_arr->length());
    EXPECT_TRUE(out_arr->type()->ToString() == expect_arr->type()->ToString());

    arrow::EqualOptions eqOption;
    EXPECT_TRUE(out_arr->ApproxEquals(*expect_arr, eqOption.atol(tolerance)))
        << "op: " << tc.op << ", status: " << tc.output_status
        << "\nactual: " << out_arr->ToString()
        << "\nexected: " << expect_arr->ToString();
  } else {
    EXPECT_THROW(test::RunOpAsync(ctxs, func), ::yacl::RuntimeError);
  }
}

}  // namespace scql::engine::op