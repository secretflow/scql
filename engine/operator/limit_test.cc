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

#include "engine/operator/limit.h"

#include "arrow/type.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"
#include "engine/util/spu_io.h"

namespace scql::engine::op {

struct LimitTestCase {
  std::vector<test::NamedTensor> inputs;
  pb::TensorStatus input_status;
  int64_t offset;
  int64_t count;
  std::vector<test::NamedTensor> expect_outs;
};

class LimitTest : public testing::TestWithParam<
                      std::tuple<test::SpuRuntimeTestCase, LimitTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const LimitTestCase& tc);
  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const LimitTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    LimitBatchTest, LimitTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            LimitTestCase{
                .inputs =
                    {test::NamedTensor("a", TensorFrom(arrow::boolean(),
                                                       "[true, false, true]")),
                     test::NamedTensor("b", TensorFrom(arrow::int64(),
                                                       "[1,2,null,4,5,6,7,8]")),
                     test::NamedTensor(
                         "c", TensorFrom(
                                  arrow::float64(),
                                  "[1.1025, 100.245, -10.2, 0.34, 3.1415926]")),
                     test::NamedTensor("d", TensorFrom(arrow::int64(), "[1]"))},
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .offset = 1,
                .count = 2,
                .expect_outs =
                    {test::NamedTensor("a_out", TensorFrom(arrow::boolean(),
                                                           "[false,true]")),
                     test::NamedTensor("b_out",
                                       TensorFrom(arrow::int64(), "[2,null]")),
                     test::NamedTensor("c_out", TensorFrom(arrow::float64(),
                                                           "[100.245, -10.2]")),
                     test::NamedTensor("d_out",
                                       TensorFrom(arrow::int64(), "[]"))}},
            LimitTestCase{
                .inputs =
                    {test::NamedTensor("a", TensorFrom(arrow::boolean(),
                                                       "[true, false, true]")),
                     test::NamedTensor("b", TensorFrom(arrow::int64(),
                                                       "[1,2,3,4,5,6,7,8]")),
                     test::NamedTensor(
                         "c", TensorFrom(
                                  arrow::float64(),
                                  "[1.1025, 100.245, -10.2, 0.34, 3.1415926]")),
                     test::NamedTensor("d", TensorFrom(arrow::int64(), "[1]"))},
                .input_status = pb::TENSORSTATUS_SECRET,
                .offset = 1,
                .count = 2,
                .expect_outs =
                    {test::NamedTensor("a_out", TensorFrom(arrow::boolean(),
                                                           "[false,true]")),
                     test::NamedTensor("b_out",
                                       TensorFrom(arrow::int64(), "[2,3]")),
                     test::NamedTensor("c_out", TensorFrom(arrow::float64(),
                                                           "[100.245, -10.2]")),
                     test::NamedTensor("d_out",
                                       TensorFrom(arrow::int64(), "[]"))}},
            LimitTestCase{
                .inputs =
                    {test::NamedTensor("a", TensorFrom(arrow::boolean(),
                                                       "[true, false, true]")),
                     test::NamedTensor("b", TensorFrom(arrow::int64(),
                                                       "[1,2,3,4,5,6,7,8]")),
                     test::NamedTensor(
                         "c", TensorFrom(
                                  arrow::float64(),
                                  "[1.1025, 100.245, -10.2, 0.34, 3.1415926]")),
                     test::NamedTensor("d", TensorFrom(arrow::int64(), "[1]"))},
                .input_status = pb::TENSORSTATUS_PUBLIC,
                .offset = 1,
                .count = 2,
                .expect_outs =
                    {test::NamedTensor("a_out", TensorFrom(arrow::boolean(),
                                                           "[false,true]")),
                     test::NamedTensor("b_out",
                                       TensorFrom(arrow::int64(), "[2,3]")),
                     test::NamedTensor("c_out", TensorFrom(arrow::float64(),
                                                           "[100.245, -10.2]")),
                     test::NamedTensor("d_out",
                                       TensorFrom(arrow::int64(), "[]"))}})),
    TestParamNameGenerator(LimitTest));

TEST_P(LimitTest, works) {
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
  EXPECT_NO_THROW(test::RunAsync<Limit>(ctx_ptrs));

  // Then
  // check alice output
  for (const auto& expect_t : tc.expect_outs) {
    auto expect_arr = expect_t.tensor->ToArrowChunkedArray();
    TensorPtr out = nullptr;
    if (tc.input_status == pb::TENSORSTATUS_PRIVATE) {
      out = ctx_ptrs[0]->GetTensorTable()->GetTensor(expect_t.name);
    } else if (tc.input_status == pb::TENSORSTATUS_PUBLIC) {
      auto spu_io =
          util::SpuOutfeedHelper(ctx_ptrs[0]->GetSession()->GetSpuContext(),
                                 ctx_ptrs[0]->GetSession()->GetDeviceSymbols());
      EXPECT_NO_THROW(out = spu_io.DumpPublic(expect_t.name));
    } else {
      EXPECT_NO_THROW(out = test::RevealSecret(ctx_ptrs, expect_t.name));
    }
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
/// LimitTest impl
/// ===================

pb::ExecNode LimitTest::MakeExecNode(const LimitTestCase& tc) {
  test::ExecNodeBuilder builder(Limit::kOpType);

  builder.SetNodeName("Limit-test");
  builder.AddInt64Attr(Limit::kOffset, tc.offset);
  builder.AddInt64Attr(Limit::kCount, tc.count);

  std::vector<pb::Tensor> inputs;
  for (const auto& named_tensor : tc.inputs) {
    auto t = test::MakeTensorReference(
        named_tensor.name, named_tensor.tensor->Type(), tc.input_status);
    inputs.push_back(std::move(t));
  }
  builder.AddInput(Limit::kIn, inputs);

  std::vector<pb::Tensor> outputs;
  for (const auto& named_tensor : tc.expect_outs) {
    auto t = test::MakeTensorReference(
        named_tensor.name, named_tensor.tensor->Type(), tc.input_status);
    outputs.push_back(std::move(t));
  }
  builder.AddOutput(Limit::kOut, outputs);

  return builder.Build();
}

void LimitTest::FeedInputs(const std::vector<ExecContext*>& ctxs,
                           const LimitTestCase& tc) {
  if (tc.input_status == pb::TENSORSTATUS_PRIVATE) {
    for (auto ctx : ctxs) {
      test::FeedInputsAsPrivate(ctx, tc.inputs);
    }
  } else if (tc.input_status == pb::TensorStatus::TENSORSTATUS_SECRET) {
    test::FeedInputsAsSecret(ctxs, tc.inputs);
  } else {
    test::FeedInputsAsPublic(ctxs, tc.inputs);
  }
}

}  // namespace scql::engine::op