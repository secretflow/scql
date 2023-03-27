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

#include "engine/operator/make_share.h"

#include "arrow/compute/cast.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_from_json.h"
#include "engine/core/type.h"
#include "engine/operator/test_util.h"
#include "engine/util/ndarray_to_arrow.h"
#include "engine/util/spu_io.h"

namespace scql::engine::op {

struct MakeShareTestCase {
  std::vector<test::NamedTensor> inputs;
  // private input inputs[i] owns by party who's rank = owners[i]
  std::vector<size_t> owners;
  std::vector<std::string> output_names;
};

class MakeShareTest : public testing::TestWithParam<
                          std::tuple<spu::ProtocolKind, MakeShareTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const MakeShareTestCase& tc);
  static void FeedInputs(ExecContext* ctx, const MakeShareTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    MakeShareBatchTest, MakeShareTest,
    testing::Combine(
        testing::Values(spu::ProtocolKind::CHEETAH, spu::ProtocolKind::SEMI2K),
        testing::Values(
            MakeShareTestCase{
                .inputs =
                    {test::NamedTensor(
                         "x", TensorFromJSON(arrow::utf8(),
                                             R"json(["A", "B", "C"])json")),
                     test::NamedTensor(
                         "y", TensorFromJSON(arrow::utf8(),
                                             R"json(["X", "Y", "Z"])json"))},
                .owners = {0, 1},
                .output_names = {"x_hat", "y_hat"}},
            MakeShareTestCase{
                .inputs =
                    {test::NamedTensor("x",
                                       TensorFromJSON(arrow::int64(),
                                                      "[1,2,3,4,5,6,7,8]")),
                     test::NamedTensor(
                         "y",
                         TensorFromJSON(
                             arrow::float32(),
                             "[1.1025, 100.245, -10.2, 0.34, 3.1415926]"))},
                .owners = {0, 1},
                .output_names = {"x_hat", "y_hat"}},
            MakeShareTestCase{
                .inputs =
                    {test::NamedTensor(
                         "x", TensorFromJSON(arrow::boolean(),
                                             "[true, false, true, true]")),
                     test::NamedTensor(
                         "y", TensorFromJSON(arrow::boolean(),
                                             "[false, false, false, true]"))},
                .owners = {0, 1},
                .output_names = {"x_hat", "y_hat"}},
            MakeShareTestCase{
                .inputs = {test::NamedTensor("x", TensorFromJSON(arrow::int64(),
                                                                 "[]")),
                           test::NamedTensor(
                               "y", TensorFromJSON(arrow::boolean(), "[]"))},
                .owners = {0, 1},
                .output_names = {"x_hat", "y_hat"}})),
    TestParamNameGenerator(MakeShareTest));

TEST_P(MakeShareTest, works) {
  // Given
  auto parm = GetParam();
  auto tc = std::get<1>(parm);
  auto node = MakeExecNode(tc);
  std::vector<Session> sessions = test::Make2PCSession(std::get<0>(parm));

  ExecContext alice_ctx(node, &sessions[0]);
  ExecContext bob_ctx(node, &sessions[1]);

  // feed inputs
  FeedInputs(&alice_ctx, tc);
  FeedInputs(&bob_ctx, tc);

  // When
  test::OperatorTestRunner<MakeShare> alice;
  test::OperatorTestRunner<MakeShare> bob;

  alice.Start(&alice_ctx);
  bob.Start(&bob_ctx);

  // Then
  EXPECT_NO_THROW({ alice.Wait(); });
  EXPECT_NO_THROW({ bob.Wait(); });

  for (size_t i = 0; i < tc.output_names.size(); ++i) {
    TensorPtr t = nullptr;
    EXPECT_NO_THROW({
      t = test::RevealSecret({&alice_ctx, &bob_ctx}, tc.output_names[i]);
    });
    // convert hash to string for string tensor in spu
    if (tc.inputs[i].tensor->Type() == pb::PrimitiveDataType::STRING) {
      auto ctx = tc.owners[i] == 0 ? alice_ctx : bob_ctx;
      t = ctx.GetSession()->HashToString(*t);
    }

    auto actual_output = t->ToArrowChunkedArray();

    auto expect_output = tc.inputs[i].tensor->ToArrowChunkedArray();

    EXPECT_TRUE(actual_output->ApproxEquals(*expect_output))
        << "actual output = " << actual_output->ToString()
        << ", expect output = " << expect_output->ToString();
  }
}

/// ===================
/// MakeShareTest impl
/// ===================

pb::ExecNode MakeShareTest::MakeExecNode(const MakeShareTestCase& tc) {
  test::ExecNodeBuilder builder(MakeShare::kOpType);

  builder.SetNodeName("make-share-test");

  std::vector<pb::Tensor> inputs;
  for (const auto& named_tensor : tc.inputs) {
    auto t = test::MakePrivateTensorReference(named_tensor.name,
                                              named_tensor.tensor->Type());
    inputs.push_back(std::move(t));
  }
  builder.AddInput(MakeShare::kIn, inputs);

  std::vector<pb::Tensor> outputs;
  for (size_t i = 0; i < tc.output_names.size(); ++i) {
    auto t = test::MakeSecretTensorReference(tc.output_names[i],
                                             inputs[i].elem_type());
    outputs.push_back(std::move(t));
  }
  builder.AddOutput(MakeShare::kOut, outputs);

  return builder.Build();
}

void MakeShareTest::FeedInputs(ExecContext* ctx, const MakeShareTestCase& tc) {
  auto tensor_table = ctx->GetTensorTable();

  auto lctx = ctx->GetSession()->GetLink();
  for (size_t i = 0; i < tc.owners.size(); ++i) {
    if (lctx->Rank() == tc.owners[i]) {
      tensor_table->AddTensor(tc.inputs[i].name, tc.inputs[i].tensor);
    }
  }
}

}  // namespace scql::engine::op