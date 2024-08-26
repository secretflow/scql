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

#include "arrow/array.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/cast.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"
#include "engine/util/spu_io.h"

namespace scql::engine::op {
struct TrigonometricFunctionTestCase {
  test::NamedTensor input;
  pb::TensorStatus input_status;
  test::NamedTensor output;
  std::string op;
  bool correct;
  size_t world_size = 2;
};

const std::string Sin = "Sin";
const std::string Cos = "Cos";
const std::string ACos = "ACos";

class TrigonometricFunctionTest
    : public ::testing::TestWithParam<
          std::tuple<test::SpuRuntimeTestCase, TrigonometricFunctionTestCase>> {
 protected:
  static pb::ExecNode MakeTrigonometricFunctionExecNode(
      const TrigonometricFunctionTestCase& tc, const std::string& name);
  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const TrigonometricFunctionTestCase& tc);
};

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

void TrigonometricFunctionTest::FeedInputs(
    const std::vector<ExecContext*>& ctxs,
    const TrigonometricFunctionTestCase& tc) {
  if (tc.input_status == pb::TensorStatus::TENSORSTATUS_PRIVATE) {
    for (auto ctx : ctxs) {
      test::FeedInputsAsPrivate(ctx, {tc.input});
    }
  } else if (tc.input_status == pb::TensorStatus::TENSORSTATUS_SECRET) {
    test::FeedInputsAsSecret(ctxs, {tc.input});
  } else {
    test::FeedInputsAsPublic(ctxs, {tc.input});
  }
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
                .op = Sin,
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
                .op = Cos,
                .correct = true},
            TrigonometricFunctionTestCase{
                .input = test::NamedTensor(
                    "in", TensorFrom(arrow::float64(), "[1.0, 0.0, -1.0]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output = test::NamedTensor(
                    "out", TensorFrom(arrow::float64(),
                                      "[0.0, 1.57079632679, 3.14159265359]")),
                .op = ACos,
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
                .op = Sin,
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
                .op = Cos,
                .correct = true},
            TrigonometricFunctionTestCase{
                .input = test::NamedTensor("in", TensorFrom(arrow::int32(),
                                                            "[0, 1]")),
                .input_status = pb::TENSORSTATUS_SECRET,
                .output = test::NamedTensor(
                    "out", TensorFrom(arrow::float64(), "[1.0, 0.54030231]")),
                .op = Cos,
                .correct = true},
            TrigonometricFunctionTestCase{
                .input = test::NamedTensor(
                    "in", TensorFrom(arrow::float64(), "[1.0, 0.0, -1.0]")),
                .input_status = pb::TENSORSTATUS_SECRET,
                .output = test::NamedTensor(
                    "out", TensorFrom(arrow::float64(),
                                      "[0.0, 1.57079632679, 3.14159265359]")),
                .op = ACos,
                .correct = true},
            TrigonometricFunctionTestCase{
                .input = test::NamedTensor("in", TensorFrom(arrow::float64(),
                                                            "[1,null]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output = test::NamedTensor("out", TensorFrom(arrow::float64(),
                                                              "[0.0,null]")),
                .op = ACos,
                .correct = true /*null input, but single party*/,
                .world_size = 1},
            TrigonometricFunctionTestCase{
                .input = test::NamedTensor(
                    "in",
                    TensorFrom(arrow::float64(), "[0.0, 0.52359878, null]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output = test::NamedTensor(
                    "out", TensorFrom(arrow::float64(), "[0.0, 0.5, null]")),
                .op = Sin,
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
                .op = Cos,
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
  for (size_t i = 0; i < sessions.size(); i++) {
    exec_ctx.emplace_back(node, sessions[i].get());
  }

  std::vector<ExecContext*> ctx_ptrs;
  for (size_t i = 0; i < exec_ctx.size(); i++) {
    ctx_ptrs.emplace_back(&exec_ctx[i]);
  }

  FeedInputs(ctx_ptrs, tc);
  std::unordered_map<std::string,
                     std::function<void(std::vector<ExecContext*>)>>
      func_handlers;
  func_handlers[Sin] = [](std::vector<ExecContext*> execs) {
    test::RunAsync<Sine>(execs);
  };
  func_handlers[Cos] = [](std::vector<ExecContext*> execs) {
    test::RunAsync<Cosine>(execs);
  };
  func_handlers[ACos] = [](std::vector<ExecContext*> execs) {
    test::RunAsync<ACosine>(execs);
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
        << out_arr;
  } else {
    SPDLOG_INFO("running {} test, expecting failure", tc.op);
    EXPECT_THROW(func_handlers[tc.op](ctx_ptrs), ::yacl::RuntimeError);
  }
}

}  // namespace scql::engine::op