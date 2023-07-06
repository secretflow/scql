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

#include "engine/operator/copy.h"

#include "arrow/type.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_from_json.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct CopyTestCase {
  std::vector<test::NamedTensor> datas;
  std::vector<std::string> output_names;
};

class CopyTest : public ::testing::TestWithParam<
                     std::tuple<test::SpuRuntimeTestCase, CopyTestCase>> {
 protected:
  static pb::ExecNode MakeCopyExecNode(const CopyTestCase& tc);

  static void FeedInputs(ExecContext* ctx, const CopyTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    CopyPrivateTest, CopyTest,
    testing::Combine(
        test::SpuTestValues2PC,
        testing::Values(
            CopyTestCase{
                .datas =
                    {test::NamedTensor(
                         "x1", TensorFromJSON(arrow::float64(),
                                              "[-3.1415, 0.1, 99.999, null]")),
                     test::NamedTensor("x2", TensorFromJSON(arrow::boolean(),
                                                            "[1,0,0,1]"))},
                .output_names = {"x1_copy", "x2_copy"}},
            CopyTestCase{
                .datas = {test::NamedTensor(
                    "x1", TensorFromJSON(arrow::utf8(),
                                         R"json(["D","C",null,"B","A"])json"))},
                .output_names = {"x1_copy"}},
            CopyTestCase{
                .datas = {test::NamedTensor(
                    "x1", TensorFromJSON(arrow::int64(),
                                         "[null,0,1,2,3,10,11,12,13]"))},
                .output_names = {"x1_copy"}})),
    TestParamNameGenerator(CopyTest));

TEST_P(CopyTest, works) {
  // Given
  auto parm = GetParam();
  auto tc = std::get<1>(parm);
  auto node = MakeCopyExecNode(tc);
  std::vector<Session> sessions = test::MakeMultiPCSession(std::get<0>(parm));

  ExecContext alice_ctx(node, &sessions[0]);
  ExecContext bob_ctx(node, &sessions[1]);

  // feed inputs, test copy from alice to bob.
  FeedInputs(&alice_ctx, tc);

  // When
  EXPECT_NO_THROW(test::RunAsync<Copy>({&alice_ctx, &bob_ctx}));

  // Then check bob output
  auto tensor_table = bob_ctx.GetTensorTable();
  for (size_t i = 0; i < tc.output_names.size(); ++i) {
    auto in_arr = tc.datas[i].tensor->ToArrowChunkedArray();
    auto out = tensor_table->GetTensor(tc.output_names[i]);
    ASSERT_TRUE(out);
    // compare tensor content
    EXPECT_TRUE(out->ToArrowChunkedArray()->Equals(in_arr))
        << "expect type = " << in_arr->type()->ToString()
        << ", got type = " << out->ToArrowChunkedArray()->type()->ToString()
        << "\nexpect result = " << in_arr->ToString()
        << "\nbut actual got result = "
        << out->ToArrowChunkedArray()->ToString();
  }
}

/// ===========================
/// CopyTest impl
/// ===========================

pb::ExecNode CopyTest::MakeCopyExecNode(const CopyTestCase& tc) {
  test::ExecNodeBuilder builder(Copy::kOpType);

  builder.SetNodeName("copy-test");
  builder.AddStringsAttr(Copy::kInputPartyCodesAttr,
                         std::vector<std::string>{test::kPartyAlice});
  builder.AddStringsAttr(Copy::kOutputPartyCodesAttr,
                         std::vector<std::string>{test::kPartyBob});
  // Add inputs
  std::vector<pb::Tensor> input_datas;
  for (const auto& named_tensor : tc.datas) {
    auto data = test::MakePrivateTensorReference(named_tensor.name,
                                                 named_tensor.tensor->Type());
    input_datas.push_back(std::move(data));
  }
  builder.AddInput(Copy::kIn, input_datas);

  // Add outputs
  std::vector<pb::Tensor> outputs;
  for (size_t i = 0; i < tc.output_names.size(); ++i) {
    auto out = test::MakeTensorAs(tc.output_names[i], input_datas[i]);
    outputs.push_back(std::move(out));
  }
  builder.AddOutput(Copy::kOut, outputs);

  return builder.Build();
}

void CopyTest::FeedInputs(ExecContext* ctx, const CopyTestCase& tc) {
  test::FeedInputsAsPrivate(ctx, tc.datas);
}

}  // namespace scql::engine::op