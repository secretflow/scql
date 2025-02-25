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

#include "engine/operator/trigonometric.h"

#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {
struct TrigonometricFunctionTestCase {
  test::NamedTensor input;
  pb::TensorStatus input_status;
  test::NamedTensor output;
  std::string op;
  bool correct;
  size_t world_size = 2;
};

struct ATan2TestCase {
  std::vector<test::NamedTensor> inputs;
  test::NamedTensor output;
  pb::TensorStatus output_status;
};

const std::string SinOp = "Sin";
const std::string CosOp = "Cos";
const std::string ACosOp = "ACos";
const std::string ASinOp = "ASin";
const std::string TanOp = "Tan";
const std::string ATanOp = "ATan";

class TrigonometricFunctionTest
    : public ::testing::TestWithParam<
          std::tuple<test::SpuRuntimeTestCase, TrigonometricFunctionTestCase>> {
 protected:
  static pb::ExecNode MakeTrigonometricFunctionExecNode(
      const TrigonometricFunctionTestCase& tc, const std::string& name);
  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const TrigonometricFunctionTestCase& tc);
};

class ATan2Test : public ::testing::TestWithParam<
                      std::tuple<test::SpuRuntimeTestCase, ATan2TestCase>> {
 protected:
  static pb::ExecNode MakeAtan2ExecNode(const ATan2TestCase& tc,
                                        const std::string& name);
  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const ATan2TestCase& tc);
};

pb::ExecNode ATan2Test::MakeAtan2ExecNode(const ATan2TestCase& tc,
                                          const std::string& name) {
  test::ExecNodeBuilder builder(name);
  builder.SetNodeName(name + "-test");
  {
    auto left = test::MakeTensorReference(
        tc.inputs[0].name, tc.inputs[0].tensor->Type(), tc.output_status);
    builder.AddInput(ATan2::kInLeft, {left});

    auto right = test::MakeTensorReference(
        tc.inputs[1].name, tc.inputs[1].tensor->Type(), tc.output_status);
    builder.AddInput(ATan2::kInRight, {right});
  }

  {
    auto t = test::MakeTensorReference(tc.output.name, tc.output.tensor->Type(),
                                       tc.output_status);
    builder.AddOutput(ATan2::kOut, {t});
  }

  return builder.Build();
}

pb::ExecNode TrigonometricFunctionTest::MakeTrigonometricFunctionExecNode(
    const TrigonometricFunctionTestCase& tc, const std::string& name) {
  test::ExecNodeBuilder builder(name);
  builder.SetNodeName(name + "-test");
  {
    auto t = test::MakeTensorReference(tc.input.name, tc.input.tensor->Type(),
                                       tc.input_status);
    builder.AddInput(TrigonometricFunction::kIn, {t});
  }

  {
    auto t = test::MakeTensorReference(tc.output.name, tc.output.tensor->Type(),
                                       tc.input_status);
    builder.AddOutput(TrigonometricFunction::kOut, {t});
  }

  return builder.Build();
}

void ATan2Test::FeedInputs(const std::vector<ExecContext*>& ctxs,
                           const ATan2TestCase& tc) {
  if (tc.output_status == pb::TENSORSTATUS_PRIVATE) {
    for (auto* ctx : ctxs) {
      test::FeedInputsAsPrivate(ctx, tc.inputs);
    }
  } else if (tc.output_status == pb::TENSORSTATUS_SECRET) {
    test::FeedInputsAsSecret(ctxs, tc.inputs);
  } else {
    test::FeedInputsAsPublic(ctxs, tc.inputs);
  }
}

void TrigonometricFunctionTest::FeedInputs(
    const std::vector<ExecContext*>& ctxs,
    const TrigonometricFunctionTestCase& tc) {
  if (tc.input_status == pb::TensorStatus::TENSORSTATUS_PRIVATE) {
    for (auto* ctx : ctxs) {
      test::FeedInputsAsPrivate(ctx, {tc.input});
    }
  } else if (tc.input_status == pb::TensorStatus::TENSORSTATUS_SECRET) {
    SPDLOG_INFO("feeding as secret");
    test::FeedInputsAsSecret(ctxs, {tc.input});
  } else {
    test::FeedInputsAsPublic(ctxs, {tc.input});
  }
}

INSTANTIATE_TEST_SUITE_P(
    ATan2Test, ATan2Test,
    testing::Combine(
        test::SpuTestValues2PC,
        testing::Values(
            ATan2TestCase{
                .inputs = {test::NamedTensor(
                               "left",
                               TensorFrom(arrow::float64(),
                                          "[1,1.73205080757]")),  // 1, sqrt(3)
                           test::NamedTensor(
                               "right", TensorFrom(arrow::float64(), "[1,1]"))},
                .output = test::NamedTensor(
                    "out",
                    TensorFrom(arrow::float64(),
                               "[0.78539816,1.0471975512]")),  // pi/4 , pi/3
                .output_status = pb::TENSORSTATUS_PRIVATE},
            ATan2TestCase{
                .inputs = {test::NamedTensor(
                               "left",
                               TensorFrom(arrow::float64(),
                                          "[1,1.73205080757]")),  // 1, sqrt(3)
                           test::NamedTensor(
                               "right", TensorFrom(arrow::float64(), "[1,1]"))},
                .output = test::NamedTensor(
                    "out",
                    TensorFrom(arrow::float64(),
                               "[0.78539816,1.0471975512]")),  // pi/4 , pi/3
                .output_status = pb::TENSORSTATUS_SECRET})));

TEST_P(ATan2Test, atan2_works) {
  auto param = GetParam();
  auto tc = std::get<1>(param);
  auto node = MakeAtan2ExecNode(tc, "atan2");
  std::vector<std::shared_ptr<scql::engine::Session>> sessions;
  sessions = test::MakeMultiPCSession(std::get<0>(param));
  std::vector<ExecContext> exec_ctx;
  exec_ctx.reserve(sessions.size());
  for (auto& session : sessions) {
    exec_ctx.emplace_back(node, session.get());
  }

  std::vector<ExecContext*> ctx_ptrs;
  ctx_ptrs.reserve(exec_ctx.size());
  for (auto& i : exec_ctx) {
    ctx_ptrs.emplace_back(&i);
  }

  FeedInputs(ctx_ptrs, tc);

  EXPECT_NO_THROW(test::RunAsync<ATan2>(ctx_ptrs));

  TensorPtr result;
  if (tc.output_status == pb::TENSORSTATUS_PRIVATE) {
    result = ctx_ptrs[0]->GetTensorTable()->GetTensor(tc.output.name);
  } else {
    EXPECT_NO_THROW(result = test::RevealSecret(ctx_ptrs, tc.output.name));
  }

  ASSERT_TRUE(result);
  auto out_arr = result->ToArrowChunkedArray();
  auto expect_arr = tc.output.tensor->ToArrowChunkedArray();

  EXPECT_TRUE(out_arr->length() == expect_arr->length());
  EXPECT_TRUE(out_arr->type()->ToString() == expect_arr->type()->ToString());

  arrow::EqualOptions eqOption;
  EXPECT_TRUE(out_arr->ApproxEquals(*expect_arr, eqOption.atol(0.001)))
      << "actual: " << out_arr << "\nexected: " << expect_arr;
}

INSTANTIATE_TEST_SUITE_P(
    TrigonometricPrivateTest, TrigonometricFunctionTest,
    testing::Combine(
        test::SpuTestValues2PC,
        testing::Values(
            TrigonometricFunctionTestCase{
                .input = test::NamedTensor(
                    "in",
                    TensorFrom(arrow::float64(),
                               "[0.0, 0.52359878, 0.78539816, 1.0472, 1.5708, "
                               "2.0944, 2.35619, 2.61799, 3.14159]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output = test::NamedTensor(
                    "out", TensorFrom(arrow::float64(),
                                      "[0.0, 0.5, 0.70710678, 0.8660254, 1.0, "
                                      "0.8660254, 0.70710678, 0.5, 0.0]")),
                .op = SinOp,
                .correct = true},
            TrigonometricFunctionTestCase{
                .input = test::NamedTensor(
                    "in",
                    TensorFrom(arrow::float64(),
                               "[0.0, 0.52359878, 0.78539816, 1.0472, 1.5708, "
                               "2.0944, 2.35619, 2.61799, 3.14159]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output = test::NamedTensor(
                    "out",
                    TensorFrom(arrow::float64(),
                               "[1.0, 0.8660254038, 0.7071067812, 0.5, 1e-16, "
                               "-0.5, -0.7071067812, -0.8660254038, -1.0]")),
                .op = CosOp,
                .correct = true},
            TrigonometricFunctionTestCase{
                .input = test::NamedTensor(
                    "in", TensorFrom(arrow::float64(), "[1.0, 0.0, -1.0]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output = test::NamedTensor(
                    "out", TensorFrom(arrow::float64(),
                                      "[0.0, 1.57079632679, 3.14159265359]")),
                .op = ACosOp,
                .correct = true},
            TrigonometricFunctionTestCase{
                .input = test::NamedTensor(
                    "in",
                    TensorFrom(arrow::float64(),
                               "[0.0, 0.52359878, 0.78539816, 1.0472, 1.5708, "
                               "2.0944, 2.35619, 2.61799, 3.14159]")),
                .input_status = pb::TENSORSTATUS_SECRET,
                .output = test::NamedTensor(
                    "out", TensorFrom(arrow::float64(),
                                      "[0.0, 0.5, 0.70710678, 0.8660254, 1.0, "
                                      "0.8660254, 0.70710678, 0.5, 0.0]")),
                .op = SinOp,
                .correct = true},
            TrigonometricFunctionTestCase{
                .input = test::NamedTensor(
                    "in",
                    TensorFrom(arrow::float64(),
                               "[0.0, 0.52359878, 0.78539816, 1.0472, 1.5708, "
                               "2.0944, 2.35619, 2.61799, 3.14159]")),
                .input_status = pb::TENSORSTATUS_SECRET,
                .output = test::NamedTensor(
                    "out",
                    TensorFrom(arrow::float64(),
                               "[1.0, 0.8660254038, 0.7071067812, 0.5, 1e-16, "
                               "-0.5, -0.7071067812, -0.8660254038, -1.0]")),
                .op = CosOp,
                .correct = true},
            TrigonometricFunctionTestCase{
                .input = test::NamedTensor("in", TensorFrom(arrow::int32(),
                                                            "[0, 1]")),
                .input_status = pb::TENSORSTATUS_SECRET,
                .output = test::NamedTensor(
                    "out", TensorFrom(arrow::float64(), "[1.0, 0.54030231]")),
                .op = CosOp,
                .correct = true},
            TrigonometricFunctionTestCase{
                .input = test::NamedTensor(
                    "in", TensorFrom(arrow::float64(), "[1.0, 0.0, -1.0]")),
                .input_status = pb::TENSORSTATUS_SECRET,
                .output = test::NamedTensor(
                    "out", TensorFrom(arrow::float64(),
                                      "[0.0, 1.57079632679, 3.14159265359]")),
                .op = ACosOp,
                .correct = true},
            TrigonometricFunctionTestCase{
                .input = test::NamedTensor("in", TensorFrom(arrow::float64(),
                                                            "[1,null]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output = test::NamedTensor("out", TensorFrom(arrow::float64(),
                                                              "[0.0,null]")),
                .op = ACosOp,
                .correct = true,
                .world_size = 1},
            TrigonometricFunctionTestCase{
                .input = test::NamedTensor(
                    "in",
                    TensorFrom(arrow::float64(), "[0.0, 0.52359878, null]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output = test::NamedTensor(
                    "out", TensorFrom(arrow::float64(), "[0.0, 0.5, null]")),
                .op = SinOp,
                .correct = true,
                .world_size = 1},
            TrigonometricFunctionTestCase{
                .input = test::NamedTensor(
                    "in",
                    TensorFrom(arrow::float64(), "[0.0, 0.52359878, null]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output = test::NamedTensor(
                    "out",
                    TensorFrom(arrow::float64(), "[1.0, 0.8660254038, null]")),
                .op = CosOp,
                .correct = true,
                .world_size = 1},
            TrigonometricFunctionTestCase{
                .input = test::NamedTensor("in", TensorFrom(arrow::float64(),
                                                            "[0.0, 0.5, 1]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output = test::NamedTensor(
                    "out",
                    TensorFrom(
                        arrow::float64(),
                        "[0.0, 0.52359877559, 1.57079632679]")),  // 0, pi/4,
                                                                  // pi/2
                .op = ASinOp,
                .correct = true,
                .world_size = 1},
            TrigonometricFunctionTestCase{
                .input = test::NamedTensor("in", TensorFrom(arrow::float64(),
                                                            "[0.0, 0.5, 1]")),
                .input_status = pb::TENSORSTATUS_SECRET,
                .output = test::NamedTensor(
                    "out", TensorFrom(arrow::float64(),
                                      "[0.0, 0.52359877559, 1.57079632679]")),
                .op = ASinOp,
                .correct = true},
            TrigonometricFunctionTestCase{
                .input = test::NamedTensor(
                    "in",
                    TensorFrom(
                        arrow::float64(),
                        "[0.0, 0.78539816339, 0.52359877559]")),  // 0, pi/4,
                                                                  // pi/6
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output = test::NamedTensor(
                    "out",
                    TensorFrom(arrow::float64(), "[0.0, 1, 0.57735026919]")),
                .op = TanOp,
                .correct = true,
                .world_size = 1},
            TrigonometricFunctionTestCase{
                .input = test::NamedTensor(
                    "in",
                    TensorFrom(arrow::float64(), "[0.0, 1, 0.57735026919]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output = test::NamedTensor(
                    "out",
                    TensorFrom(
                        arrow::float64(),
                        "[0.0, 0.78539816339, 0.52359877559]")),  // 0, pi/4,
                                                                  // pi/6
                .op = ATanOp,
                .correct = true,
                .world_size = 1})),
    TestParamNameGenerator(TrigonometricFunctionTest));

TEST_P(TrigonometricFunctionTest, works) {
  auto param = GetParam();
  auto tc = std::get<1>(param);
  auto node = MakeTrigonometricFunctionExecNode(tc, tc.op);
  std::vector<std::shared_ptr<scql::engine::Session>> sessions;
  if (tc.world_size > 1) {
    sessions = test::MakeMultiPCSession(std::get<0>(param));
  } else {
    sessions.push_back(test::Make1PCSession());
  }

  std::vector<ExecContext> exec_ctx;
  exec_ctx.reserve(sessions.size());
  for (auto& session : sessions) {
    exec_ctx.emplace_back(node, session.get());
  }

  std::vector<ExecContext*> ctx_ptrs;
  ctx_ptrs.reserve(exec_ctx.size());
  for (auto& i : exec_ctx) {
    ctx_ptrs.emplace_back(&i);
  }

  FeedInputs(ctx_ptrs, tc);
  std::unordered_map<std::string,
                     std::function<void(std::vector<ExecContext*>)>>
      func_handlers;
  func_handlers[SinOp] = [](const std::vector<ExecContext*>& execs) {
    test::RunAsync<Sine>(execs);
  };
  func_handlers[CosOp] = [](const std::vector<ExecContext*>& execs) {
    test::RunAsync<Cosine>(execs);
  };
  func_handlers[ACosOp] = [](const std::vector<ExecContext*>& execs) {
    test::RunAsync<ACosine>(execs);
  };
  func_handlers[ASinOp] = [](const std::vector<ExecContext*>& execs) {
    test::RunAsync<ASine>(execs);
  };
  func_handlers[TanOp] = [](const std::vector<ExecContext*>& execs) {
    test::RunAsync<Tan>(execs);
  };
  func_handlers[ATanOp] = [](const std::vector<ExecContext*>& execs) {
    test::RunAsync<ATan>(execs);
  };

  if (tc.correct) {
    SPDLOG_INFO("running {} test\n", tc.op);
    EXPECT_NO_THROW(func_handlers[tc.op](ctx_ptrs));

    TensorPtr result;
    if (tc.input_status == pb::TENSORSTATUS_PRIVATE) {
      result = ctx_ptrs[0]->GetTensorTable()->GetTensor(tc.output.name);
    } else {
      EXPECT_NO_THROW(result = test::RevealSecret(ctx_ptrs, tc.output.name));
    }

    ASSERT_TRUE(result);
    auto out_arr = result->ToArrowChunkedArray();
    auto expect_arr = tc.output.tensor->ToArrowChunkedArray();

    EXPECT_TRUE(out_arr->length() == expect_arr->length());
    EXPECT_TRUE(out_arr->type()->ToString() == expect_arr->type()->ToString());

    arrow::EqualOptions eqOption;
    EXPECT_TRUE(out_arr->ApproxEquals(*expect_arr, eqOption.atol(0.001)))
        << "actual: " << out_arr << "\nexected: " << expect_arr;
  } else {
    SPDLOG_INFO("running {} test, expecting failure", tc.op);
    EXPECT_THROW(func_handlers[tc.op](ctx_ptrs), ::yacl::RuntimeError);
  }
}

}  // namespace scql::engine::op