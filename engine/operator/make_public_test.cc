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

#include "engine/operator/make_public.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"
#include "engine/util/spu_io.h"

namespace scql::engine::op {

struct MakePublicTestCase {
  std::vector<test::NamedTensor> inputs;
  pb::TensorStatus input_status;
  std::vector<std::string> output_names;
};

class MakePublicTest
    : public testing::TestWithParam<
          std::tuple<test::SpuRuntimeTestCase, MakePublicTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const MakePublicTestCase& tc);
  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const MakePublicTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    MakePublicBatchTest, MakePublicTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            MakePublicTestCase{
                .inputs =
                    {test::NamedTensor("x", TensorFrom(arrow::int64(),
                                                       "[1,2,3,4,5,6,7,8]")),
                     test::NamedTensor(
                         "y", TensorFrom(
                                  arrow::float32(),
                                  "[1.1025, 100.245, -10.2, 0.34, 3.1415926]")),
                     test::NamedTensor(
                         "z", TensorFrom(arrow::large_utf8(),
                                         R"json(["A", "B", "C"])json"))},
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output_names = {"x_hat", "y_hat", "z_hat"}},
            MakePublicTestCase{
                .inputs =
                    {test::NamedTensor(
                         "x", TensorFrom(arrow::int64(), "[1,2,3,4,5,6,7,8]")),
                     test::NamedTensor(
                         "y",
                         TensorFrom(
                             arrow::float32(),
                             "[1.1025, 100.245, -10.2, 0.34, 3.1415926]"))},
                .input_status = pb::TENSORSTATUS_SECRET,
                .output_names = {"x_hat", "y_hat"}})),
    TestParamNameGenerator(MakePublicTest));

TEST_P(MakePublicTest, works) {
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
  EXPECT_NO_THROW(test::RunAsync<MakePublic>(ctx_ptrs));

  auto check_out = [&](ExecContext* ctx, TensorPtr in,
                       const std::string& out_name) {
    auto input_arr = in->ToArrowChunkedArray();
    ASSERT_NE(nullptr, input_arr);

    auto sctx = ctx->GetSession()->GetSpuContext();
    auto device_symbols = ctx->GetSession()->GetDeviceSymbols();
    util::SpuOutfeedHelper outfeed_helper(sctx, device_symbols);
    auto output = outfeed_helper.DumpPublic(out_name);
    ASSERT_NE(nullptr, output) << "Output name: " << out_name;
    // convert hash to string for string tensor in spu
    if (in->Type() == pb::PrimitiveDataType::STRING) {
      output = ctx->GetSession()->HashToString(*output);
    }
    auto output_arr = output->ToArrowChunkedArray();
    ASSERT_NE(nullptr, output_arr);

    EXPECT_EQ(output_arr->type(), input_arr->type());
    EXPECT_TRUE(output_arr->ApproxEquals(*input_arr))
        << "actual output = " << output_arr->ToString()
        << ", expect = " << input_arr->ToString();
  };

  for (size_t i = 0; i < tc.output_names.size(); ++i) {
    const auto& output_name = tc.output_names[i];
    for (size_t idx = 0; idx < ctx_ptrs.size(); ++idx) {
      if (tc.inputs[i].tensor->Type() == pb::PrimitiveDataType::STRING &&
          idx > 0) {
        continue;  // only alice know the origin string of hash
      }
      check_out(ctx_ptrs[idx], tc.inputs[i].tensor, output_name);
    }
  }
}

/// ===================
/// MakePublicTest impl
/// ===================

pb::ExecNode MakePublicTest::MakeExecNode(const MakePublicTestCase& tc) {
  test::ExecNodeBuilder builder(MakePublic::kOpType);

  builder.SetNodeName("make-public-test");

  std::vector<pb::Tensor> inputs;
  for (const auto& named_tensor : tc.inputs) {
    pb::Tensor t;
    if (tc.input_status == pb::TENSORSTATUS_PRIVATE) {
      t = test::MakePrivateTensorReference(named_tensor.name,
                                           named_tensor.tensor->Type());
    } else {
      t = test::MakeSecretTensorReference(named_tensor.name,
                                          named_tensor.tensor->Type());
    }
    inputs.push_back(std::move(t));
  }
  builder.AddInput(MakePublic::kIn, inputs);

  std::vector<pb::Tensor> outputs;
  for (size_t i = 0; i < tc.output_names.size(); ++i) {
    auto t = test::MakeTensorReference(
        tc.output_names[i], inputs[i].elem_type(), pb::TENSORSTATUS_PUBLIC);
    outputs.push_back(std::move(t));
  }
  builder.AddOutput(MakePublic::kOut, outputs);

  return builder.Build();
}

void MakePublicTest::FeedInputs(const std::vector<ExecContext*>& ctxs,
                                const MakePublicTestCase& tc) {
  if (tc.input_status == pb::TENSORSTATUS_PRIVATE) {
    test::FeedInputsAsPrivate(ctxs[0], tc.inputs);
  } else {
    test::FeedInputsAsSecret(ctxs, tc.inputs);
  }
}

}  // namespace scql::engine::op