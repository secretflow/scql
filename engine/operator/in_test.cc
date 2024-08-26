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

#include <cstdint>
#include <vector>

#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"
#include "engine/util/psi_helper.h"

namespace scql::engine::op {

struct InTestCase {
  test::NamedTensor left_input;
  test::NamedTensor right_input;
  int64_t in_algo;
  int64_t in_type;
  std::string left_party;
  std::string right_party;
  std::string reveal_to;
  test::NamedTensor output;
  int64_t ub_server;
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
            // EcdhPsi
            InTestCase{
                .left_input = test::NamedTensor(
                    "x",
                    TensorFrom(arrow::int64(), "[10, 100, 2, -1, 34, 42]")),
                .right_input = test::NamedTensor(
                    "y",
                    TensorFrom(arrow::int64(), "[1, 2, 3, -1, 34, 43, 99]")),
                .in_algo = static_cast<int64_t>(util::PsiAlgo::kEcdhPsi),
                .left_party = test::kPartyAlice,
                .right_party = test::kPartyBob,
                .reveal_to = test::kPartyAlice,
                .output = test::NamedTensor(
                    "z",
                    TensorFrom(arrow::boolean(),
                               "[false, false, true, true, true, false]"))},
            InTestCase{
                .left_input = test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(),
                                    R"json(["A", "E", "D", "F", "A"])json")),
                .right_input = test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["C", "D", "B", "A", "G"])json")),
                .in_algo = static_cast<int64_t>(util::PsiAlgo::kEcdhPsi),
                .left_party = test::kPartyAlice,
                .right_party = test::kPartyBob,
                .reveal_to = test::kPartyAlice,
                .output = test::NamedTensor(
                    "z", TensorFrom(arrow::boolean(),
                                    "[true, false, true, false, true]"))},
            // testcase: right input empty
            InTestCase{
                .left_input = test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(),
                                    R"json(["A", "E", "D", "F", "A"])json")),
                .right_input = test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(), R"json([])json")),
                .in_algo = static_cast<int64_t>(util::PsiAlgo::kEcdhPsi),
                .left_party = test::kPartyAlice,
                .right_party = test::kPartyBob,
                .reveal_to = test::kPartyAlice,
                .output = test::NamedTensor(
                    "z", TensorFrom(arrow::boolean(),
                                    "[false, false, false, false, false]"))},
            // testcase: left input empty
            InTestCase{
                .left_input = test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(), R"json([])json")),
                .right_input = test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["A", "E", "D", "F", "A"])json")),
                .in_algo = static_cast<int64_t>(util::PsiAlgo::kEcdhPsi),
                .left_party = test::kPartyAlice,
                .right_party = test::kPartyBob,
                .reveal_to = test::kPartyAlice,
                .output = test::NamedTensor("z", TensorFrom(arrow::boolean(),
                                                            "[]"))},

            // unbalanced PSI
            // reveal to client
            InTestCase{
                .left_input = test::NamedTensor(
                    "x",
                    TensorFrom(arrow::int64(), "[10, 100, 2, -1, 34, 42]")),
                .right_input = test::NamedTensor(
                    "y",
                    TensorFrom(arrow::int64(), "[1, 2, 3, -1, 34, 43, 99]")),
                .in_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .left_party = test::kPartyAlice,
                .right_party = test::kPartyBob,
                .reveal_to = test::kPartyAlice,
                .output = test::NamedTensor(
                    "z", TensorFrom(arrow::boolean(),
                                    "[false, false, true, true, true, false]")),
                .ub_server = 1},
            InTestCase{
                .left_input = test::NamedTensor(
                    "x", TensorFrom(
                             arrow::large_utf8(),
                             R"json(["A", "E", "D", "F", "D", "A", "C"])json")),
                .right_input = test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["C", "D", "B", "A", "G"])json")),
                .in_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .left_party = test::kPartyAlice,
                .right_party = test::kPartyBob,
                .reveal_to = test::kPartyAlice,
                .output = test::NamedTensor(
                    "z",
                    TensorFrom(arrow::boolean(),
                               "[true, false, true, false, true, true, true]")),
                .ub_server = 1},
            InTestCase{
                .left_input = test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(),
                                    R"json(["A", "E", "D", "F", "A"])json")),
                .right_input = test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(), R"json([])json")),
                .in_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .left_party = test::kPartyAlice,
                .right_party = test::kPartyBob,
                .reveal_to = test::kPartyAlice,
                .output = test::NamedTensor(
                    "z", TensorFrom(arrow::boolean(),
                                    "[false, false, false, false, false]")),
                .ub_server = 1},
            InTestCase{
                .left_input = test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(), R"json([])json")),
                .right_input = test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["A", "E", "D", "F", "A"])json")),
                .in_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .left_party = test::kPartyAlice,
                .right_party = test::kPartyBob,
                .reveal_to = test::kPartyAlice,
                .output = test::NamedTensor("z",
                                            TensorFrom(arrow::boolean(), "[]")),
                .ub_server = 1},

            // reveal to server
            InTestCase{
                .left_input = test::NamedTensor(
                    "x",
                    TensorFrom(arrow::int64(), "[10, 100, 2, -1, 34, 42]")),
                .right_input = test::NamedTensor(
                    "y",
                    TensorFrom(arrow::int64(), "[1, 2, 3, -1, 34, 43, 99]")),
                .in_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .left_party = test::kPartyAlice,
                .right_party = test::kPartyBob,
                .reveal_to = test::kPartyAlice,
                .output = test::NamedTensor(
                    "z", TensorFrom(arrow::boolean(),
                                    "[false, false, true, true, true, false]")),
                .ub_server = 0},
            InTestCase{
                .left_input = test::NamedTensor(
                    "x", TensorFrom(
                             arrow::large_utf8(),
                             R"json(["A", "E", "D", "F", "D", "A", "C"])json")),
                .right_input = test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["C", "D", "B", "A", "G"])json")),
                .in_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .left_party = test::kPartyAlice,
                .right_party = test::kPartyBob,
                .reveal_to = test::kPartyAlice,
                .output = test::NamedTensor(
                    "z",
                    TensorFrom(arrow::boolean(),
                               "[true, false, true, false, true, true, true]")),
                .ub_server = 0},
            InTestCase{
                .left_input = test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(),
                                    R"json(["A", "E", "D", "F", "A"])json")),
                .right_input = test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(), R"json([])json")),
                .in_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .left_party = test::kPartyAlice,
                .right_party = test::kPartyBob,
                .reveal_to = test::kPartyAlice,
                .output = test::NamedTensor(
                    "z", TensorFrom(arrow::boolean(),
                                    "[false, false, false, false, false]")),
                .ub_server = 0},
            InTestCase{
                .left_input = test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(), R"json([])json")),
                .right_input = test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["A", "E", "D", "F", "A"])json")),
                .in_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .left_party = test::kPartyAlice,
                .right_party = test::kPartyBob,
                .reveal_to = test::kPartyAlice,
                .output = test::NamedTensor("z",
                                            TensorFrom(arrow::boolean(), "[]")),
                .ub_server = 0},

            // witout hint
            InTestCase{
                .left_input = test::NamedTensor(
                    "x",
                    TensorFrom(arrow::int64(), "[10, 100, 2, -1, 34, 42]")),
                .right_input = test::NamedTensor(
                    "y",
                    TensorFrom(arrow::int64(), "[1, 2, 3, -1, 34, 43, 99]")),
                .in_algo = static_cast<int64_t>(util::PsiAlgo::kAutoPsi),
                .left_party = test::kPartyAlice,
                .right_party = test::kPartyBob,
                .reveal_to = test::kPartyAlice,
                .output = test::NamedTensor(
                    "z", TensorFrom(arrow::boolean(),
                                    "[false, false, true, true, true, false]")),
                .ub_server = -1},
            InTestCase{
                .left_input = test::NamedTensor(
                    "x", TensorFrom(
                             arrow::large_utf8(),
                             R"json(["A", "E", "D", "F", "D", "A", "C"])json")),
                .right_input = test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["C", "D", "B", "A", "G"])json")),
                .in_algo = static_cast<int64_t>(util::PsiAlgo::kAutoPsi),
                .left_party = test::kPartyAlice,
                .right_party = test::kPartyBob,
                .reveal_to = test::kPartyAlice,
                .output = test::NamedTensor(
                    "z",
                    TensorFrom(arrow::boolean(),
                               "[true, false, true, false, true, true, true]")),
                .ub_server = -1},
            InTestCase{
                .left_input = test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(),
                                    R"json(["A", "E", "D", "F", "A"])json")),
                .right_input = test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(), R"json([])json")),
                .in_algo = static_cast<int64_t>(util::PsiAlgo::kAutoPsi),
                .left_party = test::kPartyAlice,
                .right_party = test::kPartyBob,
                .reveal_to = test::kPartyAlice,
                .output = test::NamedTensor(
                    "z", TensorFrom(arrow::boolean(),
                                    "[false, false, false, false, false]")),
                .ub_server = -1},
            InTestCase{
                .left_input = test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(), R"json([])json")),
                .right_input = test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["A", "E", "D", "F", "A"])json")),
                .in_algo = static_cast<int64_t>(util::PsiAlgo::kAutoPsi),
                .left_party = test::kPartyAlice,
                .right_party = test::kPartyBob,
                .reveal_to = test::kPartyAlice,
                .output = test::NamedTensor("z",
                                            TensorFrom(arrow::boolean(), "[]")),
                .ub_server = -1})),
    TestParamNameGenerator(InTest));

TEST_P(InTest, Works) {
  // Given
  auto parm = GetParam();
  auto tc = std::get<1>(parm);
  std::unordered_set<int64_t> supported_algos{
      static_cast<int64_t>(util::PsiAlgo::kAutoPsi),
      static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
      static_cast<int64_t>(util::PsiAlgo::kEcdhPsi)};
  if (supported_algos.count(tc.in_algo) == 0) {
    FAIL() << "In algorithm " << tc.in_algo << " not supported yet";
  }
  auto node = MakeExecNode(tc);
  auto sessions = test::MakeMultiPCSession(std::get<0>(parm));

  ExecContext alice_ctx(node, sessions[0].get());
  ExecContext bob_ctx(node, sessions[1].get());

  FeedInputs({&alice_ctx, &bob_ctx}, tc);

  // When
  EXPECT_NO_THROW(test::RunAsync<In>({&alice_ctx, &bob_ctx}));

  // Then
  if (supported_algos.count(tc.in_algo) > 0) {
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

  // set in_algo
  builder.AddInt64Attr(In::kAlgorithmAttr, tc.in_algo);
  builder.AddInt64Attr(In::kInType, tc.in_type);

  // Add input left
  auto left = test::MakePrivateTensorReference(tc.left_input.name,
                                               tc.left_input.tensor->Type());
  builder.AddInput(In::kInLeft, std::vector<pb::Tensor>{left});
  // Add input right
  auto right = test::MakePrivateTensorReference(tc.right_input.name,
                                                tc.right_input.tensor->Type());
  builder.AddInput(In::kInRight, std::vector<pb::Tensor>{right});
  // Add output
  auto output = test::MakePrivateTensorReference(tc.output.name,
                                                 tc.output.tensor->Type());
  builder.AddOutput(In::kOut, std::vector<pb::Tensor>{output});

  builder.AddStringsAttr(
      In::kInputPartyCodesAttr,
      std::vector<std::string>{tc.left_party, tc.right_party});
  builder.AddStringAttr(In::kRevealToAttr, tc.reveal_to);

  builder.AddInt64Attr(In::kUbPsiServerHint, tc.ub_server);

  return builder.Build();
}

void InTest::FeedInputs(const std::vector<ExecContext*>& ctxs,
                        const InTestCase& tc) {
  for (const auto& ctx : ctxs) {
    auto party_code = ctx->GetSession()->SelfPartyCode();
    if (party_code == tc.left_party) {
      test::FeedInputsAsPrivate(ctx, {tc.left_input});
    } else if (party_code == tc.right_party) {
      test::FeedInputsAsPrivate(ctx, {tc.right_input});
    }
  }
}

}  // namespace scql::engine::op
