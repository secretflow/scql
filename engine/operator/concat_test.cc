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

#include "engine/operator/concat.h"

#include "arrow/type.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct ConcatTestCase {
  std::vector<test::NamedTensor> inputs;
  test::NamedTensor expect_out;
};

class ConcatTest : public testing::TestWithParam<
                       std::tuple<test::SpuRuntimeTestCase, ConcatTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const ConcatTestCase& tc);
  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const ConcatTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    ConcatBatchTest, ConcatTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            ConcatTestCase{
                .inputs = {test::NamedTensor("a", TensorFrom(arrow::int64(),
                                                             "[1, 2]")),
                           test::NamedTensor("b", TensorFrom(arrow::int64(),
                                                             "[3]"))},
                .expect_out = test::NamedTensor(
                    "out", TensorFrom(arrow::int64(), "[1, 2, 3]"))},
            ConcatTestCase{
                .inputs = {test::NamedTensor(
                               "a", TensorFrom(arrow::large_utf8(),
                                               R"json(["A", "B", "C"])json")),
                           test::NamedTensor("b",
                                             TensorFrom(arrow::large_utf8(),
                                                        R"json([])json"))},
                .expect_out = test::NamedTensor(
                    "out", TensorFrom(arrow::large_utf8(),
                                      R"json(["A", "B", "C"])json"))})),
    TestParamNameGenerator(ConcatTest));

TEST_P(ConcatTest, works) {
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
  EXPECT_NO_THROW(test::RunAsync<Concat>(ctx_ptrs));

  // Then
  // check alice output
  TensorPtr t = nullptr;
  EXPECT_NO_THROW(t = test::RevealSecret(ctx_ptrs, tc.expect_out.name));
  ASSERT_TRUE(t);
  // convert hash to string for string tensor in spu
  if (tc.expect_out.tensor->Type() == pb::PrimitiveDataType::STRING) {
    t = ctx_ptrs[0]->GetSession()->HashToString(*t);
  }
  auto out_arr = t->ToArrowChunkedArray();

  auto expect_arr = tc.expect_out.tensor->ToArrowChunkedArray();

  // compare tensor content
  EXPECT_TRUE(out_arr->Equals(expect_arr))
      << "expect type = " << expect_arr->type()->ToString()
      << ", got type = " << out_arr->type()->ToString()
      << "\nexpect result = " << expect_arr->ToString()
      << "\nbut actual got result = " << out_arr->ToString();
}

/// ===================
/// ConcatTest impl
/// ===================

pb::ExecNode ConcatTest::MakeExecNode(const ConcatTestCase& tc) {
  test::ExecNodeBuilder builder(Concat::kOpType);

  builder.SetNodeName("concat-test");
  builder.AddInt64Attr(Concat::kAxis, 0);

  std::vector<pb::Tensor> inputs;
  for (const auto& named_tensor : tc.inputs) {
    auto t = test::MakeSecretTensorReference(named_tensor.name,
                                             named_tensor.tensor->Type());
    inputs.push_back(std::move(t));
  }
  builder.AddInput(Concat::kIn, inputs);

  {
    auto t = test::MakeSecretTensorReference(tc.expect_out.name,
                                             tc.expect_out.tensor->Type());
    builder.AddOutput(Concat::kOut, {t});
  }

  return builder.Build();
}

void ConcatTest::FeedInputs(const std::vector<ExecContext*>& ctxs,
                            const ConcatTestCase& tc) {
  test::FeedInputsAsSecret(ctxs, tc.inputs);
}

}  // namespace scql::engine::op