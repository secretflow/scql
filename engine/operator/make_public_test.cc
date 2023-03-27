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

#include "engine/core/tensor_from_json.h"
#include "engine/operator/test_util.h"
#include "engine/util/spu_io.h"

namespace scql::engine::op {

struct MakePublicTestCase {
  std::vector<test::NamedTensor> inputs;
  pb::TensorStatus input_status;
  std::vector<std::string> output_names;
};

class MakePublicTest : public testing::TestWithParam<
                           std::tuple<spu::ProtocolKind, MakePublicTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const MakePublicTestCase& tc);
  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const MakePublicTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    MakePublicBatchTest, MakePublicTest,
    testing::Combine(
        testing::Values(spu::ProtocolKind::CHEETAH, spu::ProtocolKind::SEMI2K),
        testing::Values(
            MakePublicTestCase{
                .inputs =
                    {test::NamedTensor("x",
                                       TensorFromJSON(arrow::int64(),
                                                      "[1,2,3,4,5,6,7,8]")),
                     test::NamedTensor(
                         "y",
                         TensorFromJSON(
                             arrow::float32(),
                             "[1.1025, 100.245, -10.2, 0.34, 3.1415926]"))},
                .input_status = pb::TENSORSTATUS_PRIVATE,
                .output_names = {"x_hat", "y_hat"}},
            MakePublicTestCase{
                .inputs =
                    {test::NamedTensor(
                         "x",
                         TensorFromJSON(arrow::int64(), "[1,2,3,4,5,6,7,8]")),
                     test::NamedTensor(
                         "y",
                         TensorFromJSON(
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
  std::vector<Session> sessions = test::Make2PCSession(std::get<0>(parm));

  ExecContext alice_ctx(node, &sessions[0]);
  ExecContext bob_ctx(node, &sessions[1]);

  // feed inputs
  FeedInputs({&alice_ctx, &bob_ctx}, tc);

  // When
  test::OperatorTestRunner<MakePublic> alice;
  test::OperatorTestRunner<MakePublic> bob;

  alice.Start(&alice_ctx);
  bob.Start(&bob_ctx);

  // Then
  EXPECT_NO_THROW({ alice.Wait(); });
  EXPECT_NO_THROW({ bob.Wait(); });

  auto check_out = [&](ExecContext* ctx, TensorPtr in,
                       const std::string& out_name) {
    auto input_arr = in->ToArrowChunkedArray();
    ASSERT_NE(nullptr, input_arr);

    auto hctx = ctx->GetSession()->GetSpuHalContext();
    auto device_symbols = ctx->GetSession()->GetDeviceSymbols();
    util::SpuOutfeedHelper outfeed_helper(hctx, device_symbols);
    auto output = outfeed_helper.DumpPublic(out_name);
    ASSERT_NE(nullptr, output) << "Output name: " << out_name;
    auto output_arr = output->ToArrowChunkedArray();
    ASSERT_NE(nullptr, output_arr);

    EXPECT_EQ(output_arr->type(), input_arr->type());
    EXPECT_TRUE(output_arr->ApproxEquals(*input_arr))
        << "actual output = " << output_arr->ToString()
        << ", expect = " << input_arr->ToString();
  };

  for (size_t i = 0; i < tc.output_names.size(); ++i) {
    const auto& output_name = tc.output_names[i];
    check_out(&alice_ctx, tc.inputs[i].tensor, output_name);
    check_out(&bob_ctx, tc.inputs[i].tensor, output_name);
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