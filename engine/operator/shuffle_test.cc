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

#include "engine/operator/shuffle.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"
#include "engine/util/psi_helper.h"

namespace scql::engine::op {

struct ShuffleTestCase {
  std::vector<test::NamedTensor> inputs;
  std::vector<std::string> output_names;
  std::vector<std::string> output_values;
};

class ShuffleTest : public testing::TestWithParam<
                        std::tuple<test::SpuRuntimeTestCase, ShuffleTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const ShuffleTestCase& tc);

  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const ShuffleTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    ShuffleBatchTest, ShuffleTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            ShuffleTestCase{
                .inputs = {test::NamedTensor("x1", TensorFrom(arrow::int64(),
                                                              "[1,2,3,4,5]")),
                           test::NamedTensor("x2",
                                             TensorFrom(arrow::int64(),
                                                        "[10,11,12,13,14]"))},
                .output_names = {"y1", "y2"},
                .output_values = {"1,10", "2,11", "3,12", "4,13", "5,14"}},
            ShuffleTestCase{.inputs = {test::NamedTensor(
                                "x1", TensorFrom(arrow::int64(), "[1]"))},
                            .output_names = {"y1"},
                            .output_values = {"1"}},
            ShuffleTestCase{.inputs = {test::NamedTensor(
                                "x1", TensorFrom(arrow::int64(), "[]"))},
                            .output_names = {"y1"},
                            .output_values = {}})),
    TestParamNameGenerator(ShuffleTest));

TEST_P(ShuffleTest, Works) {
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

  EXPECT_NO_THROW(test::RunAsync<Shuffle>(ctx_ptrs));

  std::vector<TensorPtr> outs;
  EXPECT_NO_THROW({
    for (const auto& val_name : tc.output_names) {
      auto t = test::RevealSecret(ctx_ptrs, val_name);
      ASSERT_TRUE(t != nullptr);
      EXPECT_EQ(t->Length(), tc.output_values.size());
      outs.push_back(t);
    }
  });

  util::BatchProvider provider(outs);
  auto shuffle_result = provider.ReadNextBatch();
  EXPECT_THAT(shuffle_result,
              ::testing::UnorderedElementsAreArray(tc.output_values));
}

pb::ExecNode ShuffleTest::MakeExecNode(const ShuffleTestCase& tc) {
  test::ExecNodeBuilder builder(Shuffle::kOpType);

  builder.SetNodeName("shuffle-test");
  std::vector<pb::Tensor> inputs;
  for (const auto& named_tensor : tc.inputs) {
    auto t = test::MakeSecretTensorReference(named_tensor.name,
                                             named_tensor.tensor->Type());
    inputs.push_back(std::move(t));
  }
  builder.AddInput(Shuffle::kIn, inputs);

  std::vector<pb::Tensor> outputs;
  for (size_t i = 0; i < tc.output_names.size(); ++i) {
    auto t = test::MakeTensorAs(tc.output_names[i], inputs[i]);
    outputs.push_back(std::move(t));
  }
  builder.AddOutput(Shuffle::kOut, outputs);

  return builder.Build();
}

void ShuffleTest::FeedInputs(const std::vector<ExecContext*>& ctxs,
                             const ShuffleTestCase& tc) {
  test::FeedInputsAsSecret(ctxs, tc.inputs);
}

}  // namespace scql::engine::op