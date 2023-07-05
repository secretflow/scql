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

#include "engine/operator/in.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_from_json.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

namespace {
constexpr int64_t kPsiIn = 1;
}

struct InTestCase {
  test::NamedTensor left_input;
  test::NamedTensor right_input;
  int64_t in_algo;
  std::string left_party;
  std::string right_party;
  std::string reveal_to;
  test::NamedTensor output;
};

class InTest : public ::testing::TestWithParam<
                   std::tuple<test::SpuRuntimeTestCase, InTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const InTestCase& tc);

  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const InTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    InBatchTest, InTest,
    ::testing::Combine(
        test::SpuTestValues2PC,
        testing::Values(
            InTestCase{
                .left_input = test::NamedTensor(
                    "x",
                    TensorFromJSON(arrow::int64(), "[10, 100, 2, -1, 34, 42]")),
                .right_input = test::NamedTensor(
                    "y", TensorFromJSON(arrow::int64(),
                                        "[1, 2, 3, -1, 34, 43, 99]")),
                .in_algo = kPsiIn,
                .left_party = test::kPartyAlice,
                .right_party = test::kPartyBob,
                .reveal_to = test::kPartyAlice,
                .output = test::NamedTensor(
                    "z",
                    TensorFromJSON(arrow::boolean(),
                                   "[false, false, true, true, true, false]"))},
            InTestCase{
                .left_input = test::NamedTensor(
                    "x",
                    TensorFromJSON(arrow::utf8(),
                                   R"json(["A", "E", "D", "F", "A"])json")),
                .right_input = test::NamedTensor(
                    "y",
                    TensorFromJSON(arrow::utf8(),
                                   R"json(["C", "D", "B", "A", "G"])json")),
                .in_algo = kPsiIn,
                .left_party = test::kPartyAlice,
                .right_party = test::kPartyBob,
                .reveal_to = test::kPartyBob,
                .output = test::NamedTensor(
                    "z", TensorFromJSON(arrow::boolean(),
                                        "[true, false, true, false, true]"))},
            // testcase: right input empty
            InTestCase{
                .left_input = test::NamedTensor(
                    "x",
                    TensorFromJSON(arrow::utf8(),
                                   R"json(["A", "E", "D", "F", "A"])json")),
                .right_input = test::NamedTensor(
                    "y", TensorFromJSON(arrow::utf8(), R"json([])json")),
                .in_algo = kPsiIn,
                .left_party = test::kPartyAlice,
                .right_party = test::kPartyBob,
                .reveal_to = test::kPartyBob,
                .output = test::NamedTensor(
                    "z",
                    TensorFromJSON(arrow::boolean(),
                                   "[false, false, false, false, false]"))},
            // testcase: left input empty
            InTestCase{
                .left_input = test::NamedTensor(
                    "x", TensorFromJSON(arrow::utf8(), R"json([])json")),
                .right_input = test::NamedTensor(
                    "y",
                    TensorFromJSON(arrow::utf8(),
                                   R"json(["A", "E", "D", "F", "A"])json")),
                .in_algo = kPsiIn,
                .left_party = test::kPartyAlice,
                .right_party = test::kPartyBob,
                .reveal_to = test::kPartyBob,
                .output = test::NamedTensor(
                    "z", TensorFromJSON(arrow::boolean(), "[]"))})),
    TestParamNameGenerator(InTest));

TEST_P(InTest, Works) {
  // Given
  auto parm = GetParam();
  auto tc = std::get<1>(parm);
  auto node = MakeExecNode(tc);
  std::vector<Session> sessions = test::MakeMultiPCSession(std::get<0>(parm));

  ExecContext alice_ctx(node, &sessions[0]);
  ExecContext bob_ctx(node, &sessions[1]);

  FeedInputs({&alice_ctx, &bob_ctx}, tc);

  // When
  EXPECT_NO_THROW(test::RunAsync<In>({&alice_ctx, &bob_ctx}));

  // Then
  if (tc.in_algo == kPsiIn) {
    auto tensor_table = alice_ctx.GetSession()->GetTensorTable();
    if (tc.reveal_to != test::kPartyAlice) {
      tensor_table = bob_ctx.GetSession()->GetTensorTable();
    }
    auto out_t = tensor_table->GetTensor(tc.output.name);
    ASSERT_TRUE(out_t != nullptr);
    auto out_chunked_arr = out_t->ToArrowChunkedArray();
    auto expect_chunked_arr = tc.output.tensor->ToArrowChunkedArray();
    EXPECT_TRUE(out_chunked_arr->Equals(expect_chunked_arr))
        << "\nexpect=" << expect_chunked_arr->ToString()
        << "\n,but got=" << out_chunked_arr->ToString();

  } else {
    FAIL() << "In algorithm " << tc.in_algo << " not supported yet";
  }
}

// ===============
//   InTest impl
// ===============

pb::ExecNode InTest::MakeExecNode(const InTestCase& tc) {
  test::ExecNodeBuilder builder(In::kOpType);

  builder.SetNodeName("in-test");

  builder.AddInt64Attr(In::kAlgorithmAttr, tc.in_algo);
  if (tc.in_algo == kPsiIn) {
    // Add input left
    auto left = test::MakePrivateTensorReference(tc.left_input.name,
                                                 tc.left_input.tensor->Type());
    builder.AddInput(In::kInLeft, std::vector<pb::Tensor>{left});
    // Add input right
    auto right = test::MakePrivateTensorReference(
        tc.right_input.name, tc.right_input.tensor->Type());
    builder.AddInput(In::kInRight, std::vector<pb::Tensor>{right});
    // Add output
    auto output = test::MakePrivateTensorReference(tc.output.name,
                                                   tc.output.tensor->Type());
    builder.AddOutput(In::kOut, std::vector<pb::Tensor>{output});

    builder.AddStringsAttr(
        In::kInputPartyCodesAttr,
        std::vector<std::string>{tc.left_party, tc.right_party});
    builder.AddStringAttr(In::kRevealToAttr, tc.reveal_to);
  } else {
    // TODO: support other in algorithm testcases
  }

  return builder.Build();
}

void InTest::FeedInputs(const std::vector<ExecContext*>& ctxs,
                        const InTestCase& tc) {
  if (tc.in_algo == kPsiIn) {
    for (const auto& ctx : ctxs) {
      auto party_code = ctx->GetSession()->SelfPartyCode();
      if (party_code == tc.left_party) {
        test::FeedInputsAsPrivate(ctx, {tc.left_input});
      } else if (party_code == tc.right_party) {
        test::FeedInputsAsPrivate(ctx, {tc.right_input});
      }
    }
  }
}

}  // namespace scql::engine::op
