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

#include "engine/operator/broadcast_to.h"

#include "arrow/type.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"
#include "engine/util/spu_io.h"

namespace scql::engine::op {

struct BroadcastToTestCase {
  std::vector<test::NamedTensor> inputs;
  test::NamedTensor ref_tensor;
  pb::TensorStatus ref_tensor_status;
  std::vector<test::NamedTensor> expect_outs;
};

class BroadcastToTest
    : public testing::TestWithParam<
          std::tuple<test::SpuRuntimeTestCase, BroadcastToTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const BroadcastToTestCase& tc);
  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const BroadcastToTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    BroadcastToBatchTest, BroadcastToTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            BroadcastToTestCase{
                .inputs =
                    {test::NamedTensor("a", TensorFrom(arrow::int64(), "[3]")),
                     test::NamedTensor("b",
                                       TensorFrom(arrow::float32(), "[-3.14]")),
                     test::NamedTensor("c",
                                       TensorFrom(arrow::boolean(), "[true]")),
                     test::NamedTensor("d", TensorFrom(arrow::large_utf8(),
                                                       R"json(["bob"])json"))},
                .ref_tensor = test::NamedTensor(
                    "ref", TensorFrom(arrow::int64(), "[1, 2, 3]")),
                .ref_tensor_status = pb::TENSORSTATUS_PRIVATE,
                .expect_outs =
                    {test::NamedTensor("a_out",
                                       TensorFrom(arrow::int64(), "[3, 3, 3]")),
                     test::NamedTensor("b_out",
                                       TensorFrom(arrow::float32(),
                                                  "[-3.14, -3.14, -3.14]")),
                     test::NamedTensor("c_out",
                                       TensorFrom(arrow::boolean(),
                                                  "[true, true, true]")),
                     test::NamedTensor(
                         "d_out",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["bob","bob","bob"])json"))}},
            BroadcastToTestCase{
                .inputs = {test::NamedTensor("a",
                                             TensorFrom(arrow::int64(), "[3]")),
                           test::NamedTensor("b", TensorFrom(arrow::float32(),
                                                             "[-3.14]")),
                           test::NamedTensor("c", TensorFrom(arrow::boolean(),
                                                             "[true]"))},
                .ref_tensor = test::NamedTensor(
                    "ref", TensorFrom(arrow::boolean(), "[true, false, true]")),
                .ref_tensor_status = pb::TENSORSTATUS_PUBLIC,
                .expect_outs =
                    {test::NamedTensor("a_out",
                                       TensorFrom(arrow::int64(), "[3, 3, 3]")),
                     test::NamedTensor("b_out",
                                       TensorFrom(arrow::float32(),
                                                  "[-3.14, -3.14, -3.14]")),
                     test::NamedTensor("c_out",
                                       TensorFrom(arrow::boolean(),
                                                  "[true, true, true]"))}},
            BroadcastToTestCase{
                .inputs = {test::NamedTensor("a",
                                             TensorFrom(arrow::int64(), "[3]")),
                           test::NamedTensor("b", TensorFrom(arrow::float32(),
                                                             "[-3.14]")),
                           test::NamedTensor("c", TensorFrom(arrow::boolean(),
                                                             "[true]"))},
                .ref_tensor = test::NamedTensor(
                    "ref", TensorFrom(arrow::boolean(), "[true, false, true]")),
                .ref_tensor_status = pb::TENSORSTATUS_SECRET,
                .expect_outs =
                    {test::NamedTensor("a_out",
                                       TensorFrom(arrow::int64(), "[3, 3, 3]")),
                     test::NamedTensor("b_out",
                                       TensorFrom(arrow::float32(),
                                                  "[-3.14, -3.14, -3.14]")),
                     test::NamedTensor("c_out",
                                       TensorFrom(arrow::boolean(),
                                                  "[true, true, true]"))}})),
    TestParamNameGenerator(BroadcastToTest));

TEST_P(BroadcastToTest, works) {
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
  BroadcastTo op;
  EXPECT_NO_THROW(op.Run(&exec_ctxs[0]));

  // Then
  // check alice output
  for (const auto& expect_t : tc.expect_outs) {
    TensorPtr actual_out;
    if (tc.ref_tensor_status == pb::TENSORSTATUS_PRIVATE) {
      actual_out = exec_ctxs[0].GetTensorTable()->GetTensor(expect_t.name);
    } else {
      auto sctx = exec_ctxs[0].GetSession()->GetSpuContext();
      auto device_symbols = exec_ctxs[0].GetSession()->GetDeviceSymbols();
      util::SpuOutfeedHelper outfeed_helper(sctx, device_symbols);

      actual_out = outfeed_helper.DumpPublic(expect_t.name);
    }
    ASSERT_TRUE(actual_out);
    auto actual_arr = actual_out->ToArrowChunkedArray();

    auto expect_arr = expect_t.tensor->ToArrowChunkedArray();
    // compare tensor content
    EXPECT_TRUE(actual_arr->ApproxEquals(*expect_arr))
        << "expect type = " << expect_arr->type()->ToString()
        << ", got type = " << actual_arr->type()->ToString()
        << "\nexpect result = " << expect_arr->ToString()
        << "\nbut actual got result = " << actual_arr->ToString();
  }
}

/// ===================
/// BroadcastToTest impl
/// ===================

pb::ExecNode BroadcastToTest::MakeExecNode(const BroadcastToTestCase& tc) {
  test::ExecNodeBuilder builder(BroadcastTo::kOpType);

  builder.SetNodeName("broadcast-to-test");

  std::vector<pb::Tensor> inputs;
  for (const auto& named_tensor : tc.inputs) {
    auto t = test::MakeTensorReference(named_tensor.name,
                                       named_tensor.tensor->Type(),
                                       pb::TENSORSTATUS_PUBLIC);
    inputs.push_back(std::move(t));
  }
  builder.AddInput(BroadcastTo::kIn, inputs);
  {
    auto t = test::MakeTensorReference(
        tc.ref_tensor.name, tc.ref_tensor.tensor->Type(), tc.ref_tensor_status);
    builder.AddInput(BroadcastTo::kShapeRefTensor, {t});
  }

  std::vector<pb::Tensor> outputs;
  const auto& out_status = tc.ref_tensor_status == pb::TENSORSTATUS_PRIVATE
                               ? pb::TENSORSTATUS_PRIVATE
                               : pb::TENSORSTATUS_PUBLIC;
  for (const auto& named_tensor : tc.expect_outs) {
    auto t = test::MakeTensorReference(named_tensor.name,
                                       named_tensor.tensor->Type(), out_status);
    outputs.push_back(std::move(t));
  }
  builder.AddOutput(BroadcastTo::kOut, outputs);

  return builder.Build();
}

void BroadcastToTest::FeedInputs(const std::vector<ExecContext*>& ctxs,
                                 const BroadcastToTestCase& tc) {
  test::FeedInputsAsPublic(ctxs, tc.inputs);
  if (tc.ref_tensor_status == pb::TENSORSTATUS_PRIVATE) {
    test::FeedInputsAsPrivate(ctxs[0], {tc.ref_tensor});
  } else if (tc.ref_tensor_status == pb::TENSORSTATUS_SECRET) {
    test::FeedInputsAsSecret(ctxs, {tc.ref_tensor});
  } else {
    test::FeedInputsAsPublic(ctxs, {tc.ref_tensor});
  }
}

}  // namespace scql::engine::op