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

#include "engine/operator/case_when.h"

#include "gtest/gtest.h"
#include "libspu/kernel/hal/public_helper.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"
#include "engine/util/ndarray_to_arrow.h"

namespace scql::engine::op {

struct CaseWhenTestCase {
  std::vector<test::NamedTensor> conds;
  std::vector<pb::TensorStatus> cond_status;
  std::vector<test::NamedTensor> values;
  std::vector<pb::TensorStatus> value_status;
  test::NamedTensor value_else;
  pb::TensorStatus value_else_status;
  test::NamedTensor expect_out;
  pb::TensorStatus expect_out_status;
};

class CaseWhenTest
    : public ::testing::TestWithParam<
          std::tuple<test::SpuRuntimeTestCase, CaseWhenTestCase>> {
 protected:
  static pb::ExecNode MakeCaseWhenExecNode(const CaseWhenTestCase& tc);

  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const CaseWhenTestCase& tc);
  static void FeedTensors(const std::vector<ExecContext*>& ctxs,
                          const std::vector<test::NamedTensor>& ts,
                          const std::vector<pb::TensorStatus>& status);
};

INSTANTIATE_TEST_SUITE_P(
    CaseWhenPrivateTest, CaseWhenTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            CaseWhenTestCase{
                .conds = {test::NamedTensor("cond1", TensorFrom(arrow::int64(),
                                                                "[1,0,0,0]")),
                          test::NamedTensor(
                              "cond2", TensorFrom(arrow::boolean(),
                                                  "[true,true,false,false]")),
                          test::NamedTensor("cond3",
                                            TensorFrom(arrow::int64(),
                                                       "[11,11,11,0]"))},
                .cond_status = {pb::TENSORSTATUS_PUBLIC,
                                pb::TENSORSTATUS_PUBLIC,
                                pb::TENSORSTATUS_PUBLIC},
                .values =
                    {test::NamedTensor("value1",
                                       TensorFrom(arrow::int64(), "[1,1,1,1]")),
                     test::NamedTensor("value2",
                                       TensorFrom(arrow::int64(), "[2,2,2,2]")),
                     test::NamedTensor("value3", TensorFrom(arrow::int64(),
                                                            "[3,3,3,3]"))},
                .value_status = {pb::TENSORSTATUS_PUBLIC,
                                 pb::TENSORSTATUS_PUBLIC,
                                 pb::TENSORSTATUS_PUBLIC},
                .value_else = test::NamedTensor(
                    "value_else", TensorFrom(arrow::int64(), "[4,4,4,4]")),
                .value_else_status = pb::TENSORSTATUS_PUBLIC,
                .expect_out = test::NamedTensor(
                    "out", TensorFrom(arrow::int64(), "[1,2,3,4]")),
                .expect_out_status = pb::TENSORSTATUS_PUBLIC},
            CaseWhenTestCase{
                .conds =
                    {test::NamedTensor("cond1", TensorFrom(arrow::int64(),
                                                           "[1,null,0,0]")),
                     test::NamedTensor(
                         "cond2", TensorFrom(arrow::boolean(),
                                             "[true,true,null,false]")),
                     test::NamedTensor("cond3", TensorFrom(arrow::int64(),
                                                           "[11,11,11,null]"))},
                .cond_status = {pb::TENSORSTATUS_PRIVATE,
                                pb::TENSORSTATUS_PRIVATE,
                                pb::TENSORSTATUS_PRIVATE},
                .values =
                    {test::NamedTensor("value1",
                                       TensorFrom(arrow::int64(), "[1,1,1,1]")),
                     test::NamedTensor("value2",
                                       TensorFrom(arrow::int64(), "[2,2,2,2]")),
                     test::NamedTensor("value3", TensorFrom(arrow::int64(),
                                                            "[3,3,3,3]"))},
                .value_status = {pb::TENSORSTATUS_PRIVATE,
                                 pb::TENSORSTATUS_PRIVATE,
                                 pb::TENSORSTATUS_PRIVATE},
                .value_else = test::NamedTensor(
                    "value_else", TensorFrom(arrow::int64(), "[4,4,4,4]")),
                .value_else_status = pb::TENSORSTATUS_PRIVATE,
                .expect_out = test::NamedTensor(
                    "out", TensorFrom(arrow::int64(), "[1,2,3,4]")),
                .expect_out_status = pb::TENSORSTATUS_PRIVATE},
            CaseWhenTestCase{
                .conds = {test::NamedTensor("cond1", TensorFrom(arrow::int64(),
                                                                "[1,0,0,0]")),
                          test::NamedTensor(
                              "cond2", TensorFrom(arrow::boolean(),
                                                  "[true,true,false,false]")),
                          test::NamedTensor("cond3",
                                            TensorFrom(arrow::int64(),
                                                       "[11,11,11,0]"))},
                .cond_status = {pb::TENSORSTATUS_SECRET,
                                pb::TENSORSTATUS_PUBLIC,
                                pb::TENSORSTATUS_PUBLIC},
                .values =
                    {test::NamedTensor("value1",
                                       TensorFrom(arrow::int64(), "[1,1,1,1]")),
                     test::NamedTensor("value2",
                                       TensorFrom(arrow::int64(), "[2,2,2,2]")),
                     test::NamedTensor("value3", TensorFrom(arrow::int64(),
                                                            "[3,3,3,3]"))},
                .value_status = {pb::TENSORSTATUS_SECRET,
                                 pb::TENSORSTATUS_SECRET,
                                 pb::TENSORSTATUS_PUBLIC},
                .value_else = test::NamedTensor(
                    "value_else", TensorFrom(arrow::int64(), "[4,4,4,4]")),
                .value_else_status = pb::TENSORSTATUS_PUBLIC,
                .expect_out = test::NamedTensor(
                    "out", TensorFrom(arrow::int64(), "[1,2,3,4]")),
                .expect_out_status = pb::TENSORSTATUS_SECRET},
            CaseWhenTestCase{
                .conds = {test::NamedTensor("cond1", TensorFrom(arrow::int64(),
                                                                "[-1,0,0,0]")),
                          test::NamedTensor(
                              "cond2", TensorFrom(arrow::boolean(),
                                                  "[true,true,false,false]")),
                          test::NamedTensor("cond3",
                                            TensorFrom(arrow::int64(),
                                                       "[-11,-11,-11,0]"))},
                .cond_status = {pb::TENSORSTATUS_PRIVATE,
                                pb::TENSORSTATUS_PRIVATE,
                                pb::TENSORSTATUS_PRIVATE},
                .values =
                    {test::NamedTensor(
                         "value1", TensorFrom(arrow::large_utf8(),
                                              R"json(["A","A","A","A"])json")),
                     test::NamedTensor(
                         "value2", TensorFrom(arrow::large_utf8(),
                                              R"json(["B","B","B","B"])json")),
                     test::NamedTensor(
                         "value3", TensorFrom(arrow::large_utf8(),
                                              R"json(["C","C","C","C"])json"))},
                .value_status = {pb::TENSORSTATUS_PRIVATE,
                                 pb::TENSORSTATUS_PRIVATE,
                                 pb::TENSORSTATUS_PRIVATE},
                .value_else = test::NamedTensor(
                    "value_else", TensorFrom(arrow::large_utf8(),
                                             R"json(["D","D","D","D"])json")),
                .value_else_status = pb::TENSORSTATUS_PRIVATE,
                .expect_out = test::NamedTensor(
                    "out", TensorFrom(arrow::large_utf8(),
                                      R"json(["A","B","C","D"])json")),
                .expect_out_status = pb::TENSORSTATUS_PRIVATE},
            CaseWhenTestCase{
                .conds = {test::NamedTensor("cond1", TensorFrom(arrow::int64(),
                                                                "[1,0,0,0]")),
                          test::NamedTensor(
                              "cond2", TensorFrom(arrow::boolean(),
                                                  "[true,true,false,false]")),
                          test::NamedTensor("cond3",
                                            TensorFrom(arrow::int64(),
                                                       "[-11,-11,-11,0]"))},
                .cond_status = {pb::TENSORSTATUS_SECRET,
                                pb::TENSORSTATUS_PUBLIC,
                                pb::TENSORSTATUS_PUBLIC},
                .values =
                    {test::NamedTensor(
                         "value1", TensorFrom(arrow::large_utf8(),
                                              R"json(["A","A","A","A"])json")),
                     test::NamedTensor(
                         "value2", TensorFrom(arrow::large_utf8(),
                                              R"json(["B","B","B","B"])json")),
                     test::NamedTensor(
                         "value3", TensorFrom(arrow::large_utf8(),
                                              R"json(["C","C","C","C"])json"))},
                .value_status = {pb::TENSORSTATUS_PUBLIC,
                                 pb::TENSORSTATUS_SECRET,
                                 pb::TENSORSTATUS_SECRET},
                .value_else = test::NamedTensor(
                    "value_else", TensorFrom(arrow::large_utf8(),
                                             R"json(["D","D","D","D"])json")),
                .value_else_status = pb::TENSORSTATUS_PUBLIC,
                .expect_out = test::NamedTensor(
                    "out", TensorFrom(arrow::large_utf8(),
                                      R"json(["A","B","C","D"])json")),
                .expect_out_status = pb::TENSORSTATUS_SECRET})),
    TestParamNameGenerator(CaseWhenTest));

TEST_P(CaseWhenTest, works) {
  // Given
  auto parm = GetParam();
  auto test_case = std::get<1>(parm);
  auto node = MakeCaseWhenExecNode(test_case);
  auto sessions = test::MakeMultiPCSession(std::get<0>(parm));
  std::vector<ExecContext> exec_ctxs;
  for (size_t idx = 0; idx < sessions.size(); ++idx) {
    exec_ctxs.emplace_back(node, sessions[idx].get());
  }
  std::vector<ExecContext*> ctx_ptrs;
  for (size_t idx = 0; idx < exec_ctxs.size(); ++idx) {
    ctx_ptrs.emplace_back(&exec_ctxs[idx]);
  }
  FeedInputs(ctx_ptrs, test_case);

  // When
  if (test_case.expect_out_status == pb::TENSORSTATUS_PRIVATE) {
    EXPECT_NO_THROW(test::RunAsync<CaseWhen>({ctx_ptrs[0]}));
  } else {
    EXPECT_NO_THROW(test::RunAsync<CaseWhen>(ctx_ptrs));
  }

  // Then check outputs
  auto tensor_table = ctx_ptrs[0]->GetTensorTable();

  TensorPtr out;
  if (test_case.expect_out_status == pb::TENSORSTATUS_PRIVATE) {
    EXPECT_NO_THROW(out = tensor_table->GetTensor(test_case.expect_out.name));

  } else {
    EXPECT_NO_THROW(
        out = test::RevealSecret(ctx_ptrs, test_case.expect_out.name));
    // convert hash to string for string tensor in spu
    if (test_case.expect_out.tensor->Type() == pb::PrimitiveDataType::STRING) {
      out = ctx_ptrs[0]->GetSession()->HashToString(*out);
    }
  }
  EXPECT_TRUE(out != nullptr);
  EXPECT_EQ(out->Length(), test_case.expect_out.tensor->Length());
  EXPECT_EQ(out->GetNullCount(), test_case.expect_out.tensor->GetNullCount());
  EXPECT_EQ(out->Type(), test_case.expect_out.tensor->Type());
  // compare tensor content
  EXPECT_TRUE(out->ToArrowChunkedArray()->Equals(
      test_case.expect_out.tensor->ToArrowChunkedArray()));
}

struct CaseWhenConditionTestCase {
  std::vector<test::NamedTensor> conds;
  std::vector<test::NamedTensor> expect_out;
};

class CaseWhenConditionTest
    : public ::testing::TestWithParam<
          std::tuple<test::SpuRuntimeTestCase, CaseWhenConditionTestCase>> {};

INSTANTIATE_TEST_SUITE_P(
    CaseWhenConditionShareTest, CaseWhenConditionTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            CaseWhenConditionTestCase{
                .conds = {test::NamedTensor("cond1", TensorFrom(arrow::int64(),
                                                                "[1,0,0,0]")),
                          test::NamedTensor(
                              "cond2", TensorFrom(arrow::boolean(),
                                                  "[true,true,false,false]")),
                          test::NamedTensor("cond3",
                                            TensorFrom(arrow::int64(),
                                                       "[11,11,11,0]"))},
                .expect_out =
                    {test::NamedTensor("out1",
                                       TensorFrom(arrow::boolean(),
                                                  "[true,false,false,false]")),
                     test::NamedTensor("out2",
                                       TensorFrom(arrow::boolean(),
                                                  "[false,true,false,false]")),
                     test::NamedTensor(
                         "out3", TensorFrom(arrow::boolean(),
                                            "[false,false,true,false]")),
                     test::NamedTensor(
                         "out4", TensorFrom(arrow::boolean(),
                                            "[false,false,false,true]"))}},
            CaseWhenConditionTestCase{
                .conds = {test::NamedTensor("cond1", TensorFrom(arrow::int64(),
                                                                "[-1,0,1,0]")),
                          test::NamedTensor(
                              "cond2", TensorFrom(arrow::boolean(),
                                                  "[true,false,false,false]")),
                          test::NamedTensor("cond3", TensorFrom(arrow::int64(),
                                                                "[3,3,3,3]"))},
                .expect_out =
                    {test::NamedTensor(
                         "out1", TensorFrom(arrow::boolean(),
                                            "[true,false,true,false]")),
                     test::NamedTensor(
                         "out2", TensorFrom(arrow::boolean(),
                                            "[false,false,false,false]")),
                     test::NamedTensor(
                         "out3", TensorFrom(arrow::boolean(),
                                            "[false,true,false,true]")),
                     test::NamedTensor(
                         "out4", TensorFrom(arrow::boolean(),
                                            "[false,false,false,false]"))}},
            CaseWhenConditionTestCase{
                .conds = {test::NamedTensor("cond1", TensorFrom(arrow::int64(),
                                                                "[0,0,0,0]")),
                          test::NamedTensor(
                              "cond2", TensorFrom(arrow::boolean(),
                                                  "[false,false,false,false]")),
                          test::NamedTensor("cond3", TensorFrom(arrow::int64(),
                                                                "[0,0,0,0]"))},
                .expect_out =
                    {test::NamedTensor(
                         "out1", TensorFrom(arrow::boolean(),
                                            "[false,false,false,false]")),
                     test::NamedTensor(
                         "out2", TensorFrom(arrow::boolean(),
                                            "[false,false,false,false]")),
                     test::NamedTensor(
                         "out3", TensorFrom(arrow::boolean(),
                                            "[false,false,false,false]")),
                     test::NamedTensor(
                         "out4", TensorFrom(arrow::boolean(),
                                            "[true,true,true,true]"))}})),
    TestParamNameGenerator(CaseWhenConditionTest));

TEST_P(CaseWhenConditionTest, condition) {
  // Given
  auto parm = GetParam();
  auto test_case = std::get<1>(parm);
  test::ExecNodeBuilder builder(CaseWhen::kOpType);
  builder.SetNodeName("case_when_condition-test");
  // Build node
  std::vector<pb::Tensor> conds;
  for (size_t i = 0; i < test_case.conds.size(); i++) {
    auto t = test::MakeTensorReference(test_case.conds[i].name,
                                       test_case.conds[i].tensor->Type(),
                                       pb::TENSORSTATUS_PUBLIC);
    conds.push_back(std::move(t));
  }
  builder.AddInput(CaseWhen::kCond, conds);
  auto node = builder.Build();
  auto sessions = test::MakeMultiPCSession(std::get<0>(parm));
  std::vector<ExecContext> exec_ctxs;
  for (size_t idx = 0; idx < sessions.size(); ++idx) {
    exec_ctxs.emplace_back(node, sessions[idx].get());
  }
  std::vector<ExecContext*> ctx_ptrs;
  for (size_t idx = 0; idx < exec_ctxs.size(); ++idx) {
    ctx_ptrs.emplace_back(&exec_ctxs[idx]);
  }

  test::FeedInputsAsPublic(ctx_ptrs, test_case.conds);

  auto CaseWhenOp = std::make_unique<CaseWhen>();
  auto result = CaseWhenOp->TransformConditionData(ctx_ptrs[0]);
  for (size_t i = 0; i < result.size(); i++) {
    spu::NdArrayRef arr = spu::kernel::hal::dump_public(
        ctx_ptrs[0]->GetSession()->GetSpuContext(), result[i]);
    auto arrow = util::NdArrayToArrow(arr);
    EXPECT_TRUE(arrow->Equals(
        test_case.expect_out.at(i).tensor->ToArrowChunkedArray()));
  }
}

/// ===========================
/// CaseWhenTest impl
/// ===========================

pb::ExecNode CaseWhenTest::MakeCaseWhenExecNode(const CaseWhenTestCase& tc) {
  test::ExecNodeBuilder builder(CaseWhen::kOpType);

  builder.SetNodeName("case_when-test");
  // Add inputs
  std::vector<pb::Tensor> conds;
  for (size_t i = 0; i < tc.conds.size(); i++) {
    auto t = test::MakeTensorReference(
        tc.conds[i].name, tc.conds[i].tensor->Type(), tc.cond_status[i]);
    conds.push_back(std::move(t));
  }

  builder.AddInput(CaseWhen::kCond, conds);

  std::vector<pb::Tensor> values;
  for (size_t i = 0; i < tc.values.size(); i++) {
    auto t = test::MakeTensorReference(
        tc.values[i].name, tc.values[i].tensor->Type(), tc.value_status[i]);
    values.push_back(std::move(t));
  }
  builder.AddInput(CaseWhen::kValue, values);

  auto value_else = test::MakeTensorReference(
      tc.value_else.name, tc.value_else.tensor->Type(), tc.value_else_status);
  builder.AddInput(CaseWhen::kValueElse, std::vector<pb::Tensor>{value_else});

  // Add outputs
  auto out = test::MakeTensorReference(
      tc.expect_out.name, tc.expect_out.tensor->Type(), tc.expect_out_status);
  builder.AddOutput(CaseWhen::kOut, std::vector<pb::Tensor>{out});

  return builder.Build();
}

void CaseWhenTest::FeedTensors(const std::vector<ExecContext*>& ctxs,
                               const std::vector<test::NamedTensor>& ts,
                               const std::vector<pb::TensorStatus>& status) {
  for (size_t i = 0; i < ts.size(); i++) {
    if (status[i] == pb::TENSORSTATUS_PRIVATE) {
      test::FeedInputsAsPrivate(ctxs[0], {ts[i]});
    } else if (status[i] == pb::TENSORSTATUS_SECRET) {
      test::FeedInputsAsSecret(ctxs, {ts[i]});
    } else {
      test::FeedInputsAsPublic(ctxs, {ts[i]});
    }
  }
}

void CaseWhenTest::FeedInputs(const std::vector<ExecContext*>& ctxs,
                              const CaseWhenTestCase& tc) {
  FeedTensors(ctxs, tc.conds, tc.cond_status);
  FeedTensors(ctxs, tc.values, tc.value_status);
  FeedTensors(ctxs, {tc.value_else}, {tc.value_else_status});
}

}  // namespace scql::engine::op