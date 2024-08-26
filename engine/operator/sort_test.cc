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

#include "engine/operator/sort.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct SortTestCase {
  bool reverse;
  // input and output share the same status
  pb::TensorStatus input_status;
  std::vector<test::NamedTensor> sort_keys;
  std::vector<test::NamedTensor> inputs;
  std::vector<test::NamedTensor> outputs;
};

class SortTest : public testing::TestWithParam<
                     std::tuple<test::SpuRuntimeTestCase, SortTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const SortTestCase& tc);
  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const SortTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    SortBatchTest, SortTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            SortTestCase{
                .reverse = false,
                .input_status = pb::TENSORSTATUS_SECRET,
                .sort_keys = {test::NamedTensor(
                    "k1", TensorFrom(arrow::int64(), "[5,1,2,4,3]"))},
                .inputs = {test::NamedTensor(
                    "x1", TensorFrom(arrow::int64(), "[10,11,12,13,14]"))},
                .outputs = {test::NamedTensor(
                    "y1", TensorFrom(arrow::int64(), "[11,12,14,13,10]"))}},
            SortTestCase{
                .reverse = true,
                .input_status = pb::TENSORSTATUS_SECRET,
                .sort_keys = {test::NamedTensor(
                    "k1", TensorFrom(arrow::int64(), "[5,1,2,4,3]"))},
                .inputs = {test::NamedTensor(
                    "x1", TensorFrom(arrow::int64(), "[10,11,12,13,14]"))},
                .outputs = {test::NamedTensor(
                    "y1", TensorFrom(arrow::int64(), "[10,13,14,12,11]"))}},
            SortTestCase{
                .reverse = false,
                .input_status = pb::TENSORSTATUS_SECRET,
                .sort_keys =
                    {test::NamedTensor("k1", TensorFrom(arrow::int64(),
                                                        "[2,1,2,4,3]")),
                     test::NamedTensor("k2", TensorFrom(arrow::int64(),
                                                        "[2,1,1,3,4]"))},
                .inputs = {test::NamedTensor(
                    "x1", TensorFrom(arrow::int64(), "[10,11,12,13,14]"))},
                .outputs = {test::NamedTensor(
                    "y1", TensorFrom(arrow::int64(), "[11,12,10,14,13]"))}},
            // testcase: SimpleSort can sort by multiple keys
            SortTestCase{
                .reverse = false,
                .input_status = pb::TENSORSTATUS_SECRET,
                .sort_keys =
                    {test::NamedTensor("k1", TensorFrom(arrow::int64(),
                                                        "[2,1,2,2,3]")),
                     test::NamedTensor("k2", TensorFrom(arrow::int64(),
                                                        "[3,1,1,2,4]"))},
                .inputs = {test::NamedTensor(
                    "x1", TensorFrom(arrow::int64(), "[10,11,12,13,14]"))},
                .outputs = {test::NamedTensor(
                    "y1", TensorFrom(arrow::int64(), "[11,12,13,10,14]"))}},
            // testcase: empty inputs
            SortTestCase{
                .reverse = false,
                .input_status = pb::TENSORSTATUS_SECRET,
                .sort_keys = {test::NamedTensor("k1", TensorFrom(arrow::int64(),
                                                                 "[]"))},
                .inputs = {test::NamedTensor("x1",
                                             TensorFrom(arrow::int64(), "[]"))},
                .outputs = {test::NamedTensor("y1", TensorFrom(arrow::int64(),
                                                               "[]"))}})),
    TestParamNameGenerator(SortTest));

TEST_P(SortTest, Works) {
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

  EXPECT_NO_THROW(test::RunAsync<Sort>(ctx_ptrs));

  for (const auto& named_tensor : tc.outputs) {
    TensorPtr actual_output = nullptr;
    EXPECT_NO_THROW(
        { actual_output = test::RevealSecret(ctx_ptrs, named_tensor.name); });
    ASSERT_TRUE(actual_output != nullptr);
    auto actual_arr = actual_output->ToArrowChunkedArray();
    auto expect_arr = named_tensor.tensor->ToArrowChunkedArray();
    EXPECT_TRUE(actual_arr->ApproxEquals(
        *expect_arr, arrow::EqualOptions::Defaults().atol(0.001)))
        << "\nexpect result = " << expect_arr->ToString()
        << "\nbut actual got result = " << actual_arr->ToString();
  }
}

pb::ExecNode SortTest::MakeExecNode(const SortTestCase& tc) {
  test::ExecNodeBuilder builder(Sort::kOpType);

  builder.SetNodeName("sort-test");
  std::vector<pb::Tensor> sort_keys;
  for (const auto& named_tensor : tc.sort_keys) {
    auto t = test::MakeTensorReference(
        named_tensor.name, named_tensor.tensor->Type(), tc.input_status);
    sort_keys.push_back(std::move(t));
  }
  builder.AddInput(Sort::kInKey, sort_keys);

  std::vector<pb::Tensor> inputs;
  for (const auto& named_tensor : tc.inputs) {
    auto t = test::MakeTensorReference(
        named_tensor.name, named_tensor.tensor->Type(), tc.input_status);
    inputs.push_back(std::move(t));
  }
  builder.AddInput(Sort::kIn, inputs);

  std::vector<pb::Tensor> outputs;
  for (const auto& named_tensor : tc.outputs) {
    auto t = test::MakeTensorReference(
        named_tensor.name, named_tensor.tensor->Type(), tc.input_status);
    outputs.push_back(std::move(t));
  }
  builder.AddOutput(Sort::kOut, outputs);

  builder.AddBooleanAttr(Sort::kReverseAttr, tc.reverse);

  return builder.Build();
}

void SortTest::FeedInputs(const std::vector<ExecContext*>& ctxs,
                          const SortTestCase& tc) {
  if (tc.input_status == pb::TENSORSTATUS_SECRET) {
    test::FeedInputsAsSecret(ctxs, tc.sort_keys);
    test::FeedInputsAsSecret(ctxs, tc.inputs);
  } else {
    test::FeedInputsAsPrivate(ctxs[0], tc.sort_keys);
    test::FeedInputsAsPrivate(ctxs[0], tc.inputs);
  }
}

}  // namespace scql::engine::op