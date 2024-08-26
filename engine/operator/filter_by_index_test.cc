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

#include "engine/operator/filter_by_index.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct FilterByIndexTestCase {
  test::NamedTensor indice;
  std::vector<test::NamedTensor> datas;
  std::vector<test::NamedTensor> expect_outs;
};

class FilterByIndexTest
    : public ::testing::TestWithParam<FilterByIndexTestCase> {
 protected:
  static pb::ExecNode MakeFilterByIndexExecNode(
      const FilterByIndexTestCase& tc);

  static void FeedInputs(ExecContext* ctx, const FilterByIndexTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    FilterByIndexBatchTest, FilterByIndexTest,
    testing::Values(
        FilterByIndexTestCase{
            .indice = test::NamedTensor("indice", TensorFrom(arrow::int64(),
                                                             "[3,2,1,0]")),
            .datas = {test::NamedTensor(
                          "x1", TensorFrom(arrow::large_utf8(),
                                           R"json(["A",null,"C","D"])json")),
                      test::NamedTensor("x2", TensorFrom(arrow::int64(),
                                                         "[10,11,12,13]"))},
            .expect_outs =
                {test::NamedTensor("y1",
                                   TensorFrom(arrow::large_utf8(),
                                              R"json(["D","C",null,"A"])json")),
                 test::NamedTensor("y2", TensorFrom(arrow::int64(),
                                                    "[13,12,11,10]"))}},
        FilterByIndexTestCase{
            .indice = test::NamedTensor("indice", TensorFrom(arrow::int64(),
                                                             "[3,2,2,0,null]")),
            .datas = {test::NamedTensor(
                "x1", TensorFrom(arrow::large_utf8(),
                                 R"json(["A","B","C",null,"E"])json"))},
            .expect_outs = {test::NamedTensor(
                "y1", TensorFrom(arrow::large_utf8(),
                                 R"json([null,"C","C","A",null])json"))}},
        // testcase: empty indice
        FilterByIndexTestCase{
            .indice = test::NamedTensor("indice",
                                        TensorFrom(arrow::int64(), "[]")),
            .datas = {test::NamedTensor(
                "x1", TensorFrom(arrow::large_utf8(),
                                 R"json(["A","B","C",null,"E"])json"))},
            .expect_outs = {test::NamedTensor(
                "y1", TensorFrom(arrow::large_utf8(), R"json([])json"))}}));

TEST_P(FilterByIndexTest, works) {
  // Given
  auto test_case = GetParam();
  pb::ExecNode node = MakeFilterByIndexExecNode(test_case);
  auto session = test::Make1PCSession();

  ExecContext ctx(node, session.get());

  FeedInputs(&ctx, test_case);

  // When
  FilterByIndex op;

  // Then
  EXPECT_NO_THROW({ op.Run(&ctx); });

  // check output
  auto tensor_table = ctx.GetTensorTable();
  for (const auto& expect_t : test_case.expect_outs) {
    auto out = tensor_table->GetTensor(expect_t.name);
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
/// FilterByIndexTest impl
/// ===========================

pb::ExecNode FilterByIndexTest::MakeFilterByIndexExecNode(
    const FilterByIndexTestCase& tc) {
  test::ExecNodeBuilder builder(FilterByIndex::kOpType);

  builder.SetNodeName("filter-by-index-test");
  // Add inputs
  auto indice = test::MakePrivateTensorReference(tc.indice.name,
                                                 tc.indice.tensor->Type());
  builder.AddInput(FilterByIndex::kInRowsIndexFilter,
                   std::vector<pb::Tensor>{indice});

  std::vector<pb::Tensor> input_datas;
  for (const auto& named_tensor : tc.datas) {
    auto data = test::MakePrivateTensorReference(named_tensor.name,
                                                 named_tensor.tensor->Type());
    input_datas.push_back(std::move(data));
  }
  builder.AddInput(FilterByIndex::kInData, input_datas);

  // Add outputs
  std::vector<pb::Tensor> outputs;
  for (size_t i = 0; i < tc.expect_outs.size(); ++i) {
    auto out = test::MakeTensorAs(tc.expect_outs[i].name, input_datas[i]);
    outputs.push_back(std::move(out));
  }
  builder.AddOutput(FilterByIndex::kOut, outputs);

  return builder.Build();
}

void FilterByIndexTest::FeedInputs(ExecContext* ctx,
                                   const FilterByIndexTestCase& tc) {
  auto tensor_table = ctx->GetTensorTable();
  tensor_table->AddTensor(tc.indice.name, tc.indice.tensor);
  for (const auto& named_tensor : tc.datas) {
    tensor_table->AddTensor(named_tensor.name, named_tensor.tensor);
  }
}

}  // namespace scql::engine::op
