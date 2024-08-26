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

#include "engine/operator/cast.h"

#include "arrow/type.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"
#include "engine/util/spu_io.h"

namespace scql::engine::op {

struct CastTestCase {
  test::NamedTensor input;
  pb::TensorStatus input_status;
  test::NamedTensor expect_out;
};

class CastTest : public testing::TestWithParam<
                     std::tuple<test::SpuRuntimeTestCase, CastTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const CastTestCase& tc);
  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const CastTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    CastBatchTest, CastTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            // test private status
            CastTestCase{
                .input = test::NamedTensor("in", TensorFrom(arrow::int64(),
                                                            "[1, 2, 3, null]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .expect_out = test::NamedTensor(
                    "out",
                    TensorFrom(arrow::float64(), "[1.0, 2.0, 3.0, null]"))},
            CastTestCase{
                .input = test::NamedTensor(
                    "in", TensorFrom(arrow::float64(),
                                     "[-1.5, -0.1, 1.1, 2.3, 3.5, null]")),
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .expect_out = test::NamedTensor(
                    "out",
                    TensorFrom(arrow::int64(), "[-1, 0, 1, 2, 3, null]"))},
            CastTestCase{.input = test::NamedTensor(
                             "in", TensorFrom(arrow::float64(), "[]")),
                         .input_status = pb::TENSORSTATUS_PRIVATE,
                         .expect_out = test::NamedTensor(
                             "out", TensorFrom(arrow::int64(), "[]"))},
            // test public status
            CastTestCase{
                .input = test::NamedTensor("in", TensorFrom(arrow::int64(),
                                                            "[1, 2, 3]")),
                .input_status = pb::TENSORSTATUS_PUBLIC,
                .expect_out = test::NamedTensor(
                    "out", TensorFrom(arrow::float64(), "[1.0, 2.0, 3.0]"))},
            CastTestCase{
                .input = test::NamedTensor(
                    "in", TensorFrom(arrow::float64(),
                                     "[-1.5, -0.1, 1.1, 2.3, 3.5]")),
                .input_status = pb::TENSORSTATUS_PUBLIC,
                .expect_out = test::NamedTensor(
                    "out", TensorFrom(arrow::int64(), "[-1, 0, 1, 2, 3]"))},
            CastTestCase{.input = test::NamedTensor(
                             "in", TensorFrom(arrow::float64(), "[]")),
                         .input_status = pb::TENSORSTATUS_PUBLIC,
                         .expect_out = test::NamedTensor(
                             "out", TensorFrom(arrow::int64(), "[]"))},
            // test secret status
            CastTestCase{
                .input = test::NamedTensor("in", TensorFrom(arrow::int64(),
                                                            "[1, 2, 3]")),
                .input_status = pb::TENSORSTATUS_SECRET,
                .expect_out = test::NamedTensor(
                    "out", TensorFrom(arrow::float64(), "[1.0, 2.0, 3.0]"))},
            CastTestCase{
                .input = test::NamedTensor(
                    "in", TensorFrom(arrow::float64(),
                                     "[-1.5, -0.1, 1.1, 2.3, 3.5]")),
                .input_status = pb::TENSORSTATUS_SECRET,
                .expect_out = test::NamedTensor(
                    "out", TensorFrom(arrow::int64(), "[-1, 0, 1, 2, 3]"))},
            CastTestCase{.input = test::NamedTensor(
                             "in", TensorFrom(arrow::float64(), "[]")),
                         .input_status = pb::TENSORSTATUS_SECRET,
                         .expect_out = test::NamedTensor(
                             "out", TensorFrom(arrow::int64(), "[]"))})),
    TestParamNameGenerator(CastTest));

TEST_P(CastTest, works) {
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
  EXPECT_NO_THROW(test::RunAsync<Cast>(ctx_ptrs));

  // Then
  // check alice output
  TensorPtr t = nullptr;
  if (tc.input_status == pb::TENSORSTATUS_PRIVATE) {
    t = ctx_ptrs[0]->GetTensorTable()->GetTensor(tc.expect_out.name);
  } else if (tc.input_status == pb::TENSORSTATUS_PUBLIC) {
    auto spu_io =
        util::SpuOutfeedHelper(ctx_ptrs[0]->GetSession()->GetSpuContext(),
                               ctx_ptrs[0]->GetSession()->GetDeviceSymbols());
    EXPECT_NO_THROW(t = spu_io.DumpPublic(tc.expect_out.name));
  } else {
    EXPECT_NO_THROW(t = test::RevealSecret(ctx_ptrs, tc.expect_out.name));
  }
  ASSERT_TRUE(t);
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
/// CastTest impl
/// ===================

pb::ExecNode CastTest::MakeExecNode(const CastTestCase& tc) {
  test::ExecNodeBuilder builder(Cast::kOpType);
  builder.SetNodeName("cast-test");

  {
    auto t = test::MakeTensorReference(tc.input.name, tc.input.tensor->Type(),
                                       tc.input_status);
    builder.AddInput(Cast::kIn, {t});
  }

  {
    auto t = test::MakeTensorReference(
        tc.expect_out.name, tc.expect_out.tensor->Type(), tc.input_status);
    builder.AddOutput(Cast::kOut, {t});
  }

  return builder.Build();
}

void CastTest::FeedInputs(const std::vector<ExecContext*>& ctxs,
                          const CastTestCase& tc) {
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

}  // namespace scql::engine::op