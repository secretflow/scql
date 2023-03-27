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

#include "engine/operator/filter.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_from_json.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct FilterTestCase {
  test::NamedTensor filter;
  pb::TensorStatus filter_status;
  std::vector<test::NamedTensor> datas;
  pb::TensorStatus data_status;
  std::vector<test::NamedTensor> expect_outs;
};

class FilterTest : public ::testing::TestWithParam<
                       std::tuple<spu::ProtocolKind, FilterTestCase>> {
 protected:
  static pb::ExecNode MakeFilterExecNode(const FilterTestCase& tc);

  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const FilterTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    FilterPrivateTest, FilterTest,
    testing::Combine(
        testing::Values(spu::ProtocolKind::CHEETAH, spu::ProtocolKind::SEMI2K),
        testing::Values(
            FilterTestCase{
                .filter = test::NamedTensor(
                    "filter", TensorFromJSON(arrow::boolean(), "[1,0,0,1]")),
                .filter_status = pb::TENSORSTATUS_PRIVATE,
                .datas =
                    {test::NamedTensor(
                         "x1", TensorFromJSON(arrow::utf8(),
                                              R"json(["A","B","C","D"])json")),
                     test::NamedTensor("x2", TensorFromJSON(arrow::int64(),
                                                            "[10,11,12,13]"))},
                .data_status = pb::TENSORSTATUS_PRIVATE,
                .expect_outs =
                    {test::NamedTensor(
                         "y1",
                         TensorFromJSON(arrow::utf8(), R"json(["A","D"])json")),
                     test::NamedTensor("y2", TensorFromJSON(arrow::int64(),
                                                            "[10,13]"))}},
            FilterTestCase{
                .filter = test::NamedTensor("filter",
                                            TensorFromJSON(arrow::boolean(),
                                                           "[0,0,1,1,null]")),
                .filter_status = pb::TENSORSTATUS_PUBLIC,
                .datas = {test::NamedTensor(
                    "x1", TensorFromJSON(arrow::utf8(),
                                         R"json(["A","B","C",null,"E"])json"))},
                .data_status = pb::TENSORSTATUS_PRIVATE,
                .expect_outs = {test::NamedTensor(
                    "y1",
                    TensorFromJSON(arrow::utf8(), R"json(["C",null])json"))}},
            FilterTestCase{
                .filter = test::NamedTensor("filter",
                                            TensorFromJSON(arrow::boolean(),
                                                           "[0,0,1,1,null]")),
                .filter_status = pb::TENSORSTATUS_PUBLIC,
                .datas = {test::NamedTensor(
                    "x1", TensorFromJSON(arrow::utf8(),
                                         R"json(["A","B","C","D","E"])json"))},
                .data_status = pb::TENSORSTATUS_SECRET,
                .expect_outs = {test::NamedTensor(
                    "y1",
                    TensorFromJSON(arrow::utf8(), R"json(["C","D"])json"))}},
            FilterTestCase{
                .filter = test::NamedTensor(
                    "filter", TensorFromJSON(arrow::boolean(), "[1,0,null,1]")),
                .filter_status = pb::TENSORSTATUS_PUBLIC,
                .datas = {test::NamedTensor(
                    "x1", TensorFromJSON(arrow::int64(), "[10,11,12,13]"))},
                .data_status = pb::TENSORSTATUS_SECRET,
                .expect_outs = {test::NamedTensor(
                    "y1", TensorFromJSON(arrow::int64(), "[10,13]"))}})),
    TestParamNameGenerator(FilterTest));

TEST_P(FilterTest, works) {
  // Given
  auto parm = GetParam();
  auto tc = std::get<1>(parm);
  auto node = MakeFilterExecNode(tc);
  std::vector<Session> sessions = test::Make2PCSession(std::get<0>(parm));

  ExecContext alice_ctx(node, &sessions[0]);
  ExecContext bob_ctx(node, &sessions[1]);

  // feed inputs
  FeedInputs({&alice_ctx, &bob_ctx}, tc);

  // When
  test::OperatorTestRunner<Filter> alice;
  test::OperatorTestRunner<Filter> bob;

  alice.Start(&alice_ctx);
  if (tc.data_status == pb::TENSORSTATUS_SECRET) {
    bob.Start(&bob_ctx);
  }

  // Then
  EXPECT_NO_THROW({ alice.Wait(); });
  if (tc.data_status == pb::TENSORSTATUS_SECRET) {
    EXPECT_NO_THROW({ bob.Wait(); });
  }

  // check alice's outputs
  auto tensor_table = alice_ctx.GetTensorTable();
  for (const auto& expect_t : tc.expect_outs) {
    TensorPtr out;
    if (tc.data_status == pb::TENSORSTATUS_PRIVATE) {
      EXPECT_NO_THROW(out = tensor_table->GetTensor(expect_t.name));

    } else {
      EXPECT_NO_THROW(
          out = test::RevealSecret({&alice_ctx, &bob_ctx}, expect_t.name));
      // convert hash to string for string tensor in spu
      if (expect_t.tensor->Type() == pb::PrimitiveDataType::STRING) {
        out = alice_ctx.GetSession()->HashToString(*out);
      }
    }
    EXPECT_TRUE(out != nullptr);
    EXPECT_EQ(out->Length(), expect_t.tensor->Length());
    EXPECT_EQ(out->GetNullCount(), expect_t.tensor->GetNullCount());
    EXPECT_EQ(out->Type(), expect_t.tensor->Type());
    // compare tensor content
    EXPECT_TRUE(out->ToArrowChunkedArray()->Equals(
        expect_t.tensor->ToArrowChunkedArray()));
  }
}

/// ===========================
/// FilterTest impl
/// ===========================

pb::ExecNode FilterTest::MakeFilterExecNode(const FilterTestCase& tc) {
  test::ExecNodeBuilder builder(Filter::kOpType);

  builder.SetNodeName("filter-test");
  // Add inputs
  auto filter = test::MakeTensorReference(
      tc.filter.name, tc.filter.tensor->Type(), tc.filter_status);
  builder.AddInput(Filter::kInFilter, std::vector<pb::Tensor>{filter});

  std::vector<pb::Tensor> input_datas;
  for (const auto& named_tensor : tc.datas) {
    auto data = test::MakeTensorReference(
        named_tensor.name, named_tensor.tensor->Type(), tc.data_status);
    input_datas.push_back(std::move(data));
  }
  builder.AddInput(Filter::kInData, input_datas);

  // Add outputs
  std::vector<pb::Tensor> outputs;
  for (size_t i = 0; i < tc.expect_outs.size(); ++i) {
    auto out = test::MakeTensorAs(tc.expect_outs[i].name, input_datas[i]);
    outputs.push_back(std::move(out));
  }
  builder.AddOutput(Filter::kOut, outputs);

  return builder.Build();
}

void FilterTest::FeedInputs(const std::vector<ExecContext*>& ctxs,
                            const FilterTestCase& tc) {
  if (tc.filter_status == pb::TENSORSTATUS_PRIVATE) {
    test::FeedInputsAsPrivate(ctxs[0], {tc.filter});
  } else {
    test::FeedInputsAsPublic(ctxs, {tc.filter});
  }

  if (tc.data_status == pb::TENSORSTATUS_PRIVATE) {
    test::FeedInputsAsPrivate(ctxs[0], tc.datas);
  } else {
    test::FeedInputsAsSecret(ctxs, tc.datas);
  }
}

}  // namespace scql::engine::op
