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

#include "engine/operator/if.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/if.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct IfTestCase {
  test::NamedTensor cond;
  pb::TensorStatus cond_status;
  test::NamedTensor value_true;
  pb::TensorStatus value_true_status;
  test::NamedTensor value_false;
  pb::TensorStatus value_false_status;
  test::NamedTensor out;
  pb::TensorStatus out_status;
};

class IfTest : public ::testing::TestWithParam<
                   std::tuple<test::SpuRuntimeTestCase, IfTestCase>> {
 protected:
  static pb::ExecNode MakeIfExecNode(const IfTestCase& tc);

  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const IfTestCase& tc);
  static void FeedTensors(const std::vector<ExecContext*>& ctxs,
                          const std::vector<test::NamedTensor>& ts,
                          pb::TensorStatus status);
};

INSTANTIATE_TEST_SUITE_P(
    IfPrivateTest, IfTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            IfTestCase{
                .cond = test::NamedTensor(
                    "Condition", TensorFrom(arrow::boolean(), "[1,0,1,null]")),
                .cond_status = pb::TENSORSTATUS_PRIVATE,
                .value_true = test::NamedTensor("ValueIfTrue",
                                                TensorFrom(arrow::int64(),
                                                           "[10,11,null,13]")),
                .value_true_status = pb::TENSORSTATUS_PRIVATE,
                .value_false = test::NamedTensor("ValueIfFalse",
                                                 TensorFrom(arrow::int64(),
                                                            "[16,17,18,19]")),
                .value_false_status = pb::TENSORSTATUS_PRIVATE,
                .out = test::NamedTensor(
                    "Out", TensorFrom(arrow::int64(), "[10,17,null,null]")),
                .out_status = pb::TENSORSTATUS_PRIVATE},
            IfTestCase{
                .cond = test::NamedTensor(
                    "Condition", TensorFrom(arrow::int64(), "[5,0,6,-1]")),
                .cond_status = pb::TENSORSTATUS_PRIVATE,
                .value_true = test::NamedTensor(
                    "ValueIfTrue", TensorFrom(arrow::int64(), "[10,11,12,13]")),
                .value_true_status = pb::TENSORSTATUS_PRIVATE,
                .value_false = test::NamedTensor("ValueIfFalse",
                                                 TensorFrom(arrow::int64(),
                                                            "[16,17,18,19]")),
                .value_false_status = pb::TENSORSTATUS_PRIVATE,
                .out = test::NamedTensor("Out", TensorFrom(arrow::int64(),
                                                           "[10,17,12,13]")),
                .out_status = pb::TENSORSTATUS_PRIVATE},
            IfTestCase{
                .cond = test::NamedTensor(
                    "Condition", TensorFrom(arrow::int64(), "[5,0,6,-1]")),
                .cond_status = pb::TENSORSTATUS_PRIVATE,
                .value_true = test::NamedTensor(
                    "ValueIfTrue", TensorFrom(arrow::large_utf8(),
                                              R"json(["A","B","C","D"])json")),
                .value_true_status = pb::TENSORSTATUS_PRIVATE,
                .value_false = test::NamedTensor(
                    "ValueIfFalse", TensorFrom(arrow::large_utf8(),
                                               R"json(["E","F","G","H"])json")),
                .value_false_status = pb::TENSORSTATUS_PRIVATE,
                .out = test::NamedTensor(
                    "Out", TensorFrom(arrow::large_utf8(),
                                      R"json(["A","F","C","D"])json")),
                .out_status = pb::TENSORSTATUS_PRIVATE},
            IfTestCase{
                .cond = test::NamedTensor(
                    "Condition", TensorFrom(arrow::int64(), "[5,0,6,-1]")),
                .cond_status = pb::TENSORSTATUS_SECRET,
                .value_true = test::NamedTensor(
                    "ValueIfTrue", TensorFrom(arrow::large_utf8(),
                                              R"json(["A","B","C","D"])json")),
                .value_true_status = pb::TENSORSTATUS_SECRET,
                .value_false = test::NamedTensor(
                    "ValueIfFalse", TensorFrom(arrow::large_utf8(),
                                               R"json(["E","F","G","H"])json")),
                .value_false_status = pb::TENSORSTATUS_SECRET,
                .out = test::NamedTensor(
                    "Out", TensorFrom(arrow::large_utf8(),
                                      R"json(["A","F","C","D"])json")),
                .out_status = pb::TENSORSTATUS_SECRET},
            IfTestCase{
                .cond = test::NamedTensor(
                    "Condition", TensorFrom(arrow::boolean(), "[1,0,1,1]")),
                .cond_status = pb::TENSORSTATUS_SECRET,
                .value_true = test::NamedTensor(
                    "ValueIfTrue", TensorFrom(arrow::int64(), "[10,11,12,13]")),
                .value_true_status = pb::TENSORSTATUS_SECRET,
                .value_false = test::NamedTensor("ValueIfFalse",
                                                 TensorFrom(arrow::int64(),
                                                            "[16,17,18,19]")),
                .value_false_status = pb::TENSORSTATUS_SECRET,
                .out = test::NamedTensor("Out", TensorFrom(arrow::int64(),
                                                           "[10,17,12,13]")),
                .out_status = pb::TENSORSTATUS_SECRET},
            IfTestCase{
                .cond = test::NamedTensor(
                    "Condition", TensorFrom(arrow::int64(), "[5,0,6,-1]")),
                .cond_status = pb::TENSORSTATUS_SECRET,
                .value_true = test::NamedTensor(
                    "ValueIfTrue", TensorFrom(arrow::int64(), "[10,11,12,13]")),
                .value_true_status = pb::TENSORSTATUS_SECRET,
                .value_false = test::NamedTensor("ValueIfFalse",
                                                 TensorFrom(arrow::int64(),
                                                            "[16,17,18,19]")),
                .value_false_status = pb::TENSORSTATUS_SECRET,
                .out = test::NamedTensor("Out", TensorFrom(arrow::int64(),
                                                           "[10,17,12,13]")),
                .out_status = pb::TENSORSTATUS_SECRET},
            IfTestCase{
                .cond = test::NamedTensor(
                    "Condition", TensorFrom(arrow::boolean(), "[1,0,1,1]")),
                .cond_status = pb::TENSORSTATUS_PUBLIC,
                .value_true = test::NamedTensor(
                    "ValueIfTrue", TensorFrom(arrow::int64(), "[10,11,12,13]")),
                .value_true_status = pb::TENSORSTATUS_PUBLIC,
                .value_false = test::NamedTensor("ValueIfFalse",
                                                 TensorFrom(arrow::int64(),
                                                            "[16,17,18,19]")),
                .value_false_status = pb::TENSORSTATUS_SECRET,
                .out = test::NamedTensor("Out", TensorFrom(arrow::int64(),
                                                           "[10,17,12,13]")),
                .out_status = pb::TENSORSTATUS_SECRET},
            IfTestCase{
                .cond = test::NamedTensor(
                    "Condition", TensorFrom(arrow::boolean(), "[1,0,1,1]")),
                .cond_status = pb::TENSORSTATUS_PUBLIC,
                .value_true = test::NamedTensor(
                    "ValueIfTrue", TensorFrom(arrow::int64(), "[10,11,12,13]")),
                .value_true_status = pb::TENSORSTATUS_PUBLIC,
                .value_false = test::NamedTensor("ValueIfFalse",
                                                 TensorFrom(arrow::int64(),
                                                            "[16,17,18,19]")),
                .value_false_status = pb::TENSORSTATUS_PUBLIC,
                .out = test::NamedTensor("Out", TensorFrom(arrow::int64(),
                                                           "[10,17,12,13]")),
                .out_status = pb::TENSORSTATUS_PUBLIC},
            IfTestCase{
                .cond = test::NamedTensor(
                    "Condition", TensorFrom(arrow::boolean(), "[1,0,1,1]")),
                .cond_status = pb::TENSORSTATUS_SECRET,
                .value_true = test::NamedTensor(
                    "ValueIfTrue", TensorFrom(arrow::int64(), "[10,11,12,13]")),
                .value_true_status = pb::TENSORSTATUS_SECRET,
                .value_false = test::NamedTensor(
                    "ValueIfFalse",
                    TensorFrom(arrow::float32(), "[16,17.5,18,19.5]")),
                .value_false_status = pb::TENSORSTATUS_SECRET,
                .out = test::NamedTensor("Out", TensorFrom(arrow::float32(),
                                                           "[10,17.5,12,13]")),
                .out_status = pb::TENSORSTATUS_SECRET},
            IfTestCase{
                .cond = test::NamedTensor("Condition",
                                          TensorFrom(arrow::float32(),
                                                     "[1.5,0,-1.5,1]")),
                .cond_status = pb::TENSORSTATUS_SECRET,
                .value_true = test::NamedTensor(
                    "ValueIfTrue", TensorFrom(arrow::int64(), "[10,11,12,13]")),
                .value_true_status = pb::TENSORSTATUS_SECRET,
                .value_false = test::NamedTensor(
                    "ValueIfFalse",
                    TensorFrom(arrow::float32(), "[16,17.5,18,19.5]")),
                .value_false_status = pb::TENSORSTATUS_SECRET,
                .out = test::NamedTensor("Out", TensorFrom(arrow::float32(),
                                                           "[10,17.5,12,13]")),
                .out_status = pb::TENSORSTATUS_SECRET},
            IfTestCase{
                .cond = test::NamedTensor("Condition",
                                          TensorFrom(arrow::float32(),
                                                     "[1.5,0,-1.5,1]")),
                .cond_status = pb::TENSORSTATUS_PRIVATE,
                .value_true = test::NamedTensor(
                    "ValueIfTrue", TensorFrom(arrow::int64(), "[10,11,12,13]")),
                .value_true_status = pb::TENSORSTATUS_PRIVATE,
                .value_false = test::NamedTensor(
                    "ValueIfFalse",
                    TensorFrom(arrow::float32(), "[16,17.5,18,19.5]")),
                .value_false_status = pb::TENSORSTATUS_PRIVATE,
                .out = test::NamedTensor("Out", TensorFrom(arrow::float32(),
                                                           "[10,17.5,12,13]")),
                .out_status = pb::TENSORSTATUS_PRIVATE},
            IfTestCase{
                .cond = test::NamedTensor(
                    "Condition", TensorFrom(arrow::boolean(), "[1,0,1,1]")),
                .cond_status = pb::TENSORSTATUS_PRIVATE,
                .value_true = test::NamedTensor(
                    "ValueIfTrue", TensorFrom(arrow::int64(), "[10,11,12,13]")),
                .value_true_status = pb::TENSORSTATUS_PRIVATE,
                .value_false = test::NamedTensor(
                    "ValueIfFalse",
                    TensorFrom(arrow::float32(), "[16,17.5,18,19.5]")),
                .value_false_status = pb::TENSORSTATUS_PRIVATE,
                .out = test::NamedTensor("Out", TensorFrom(arrow::float32(),
                                                           "[10,17.5,12,13]")),
                .out_status = pb::TENSORSTATUS_PRIVATE})),
    TestParamNameGenerator(IfTest));

TEST_P(IfTest, works) {
  // Given
  auto parm = GetParam();
  auto tc = std::get<1>(parm);
  auto node = MakeIfExecNode(tc);
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
  if (tc.value_false_status == pb::TENSORSTATUS_PRIVATE) {
    EXPECT_NO_THROW(test::RunAsync<If>({ctx_ptrs[0]}));
  } else {
    EXPECT_NO_THROW(test::RunAsync<If>(ctx_ptrs));
  }

  // Then check alice's outputs
  auto tensor_table = ctx_ptrs[0]->GetTensorTable();

  TensorPtr out;
  if (tc.value_false_status == pb::TENSORSTATUS_PRIVATE) {
    EXPECT_NO_THROW(out = tensor_table->GetTensor(tc.out.name));

  } else {
    EXPECT_NO_THROW(out = test::RevealSecret(ctx_ptrs, tc.out.name));
    // convert hash to string for string tensor in spu
    if (tc.out.tensor->Type() == pb::PrimitiveDataType::STRING) {
      out = ctx_ptrs[0]->GetSession()->HashToString(*out);
    }
  }
  EXPECT_TRUE(out != nullptr);
  EXPECT_EQ(out->Length(), tc.out.tensor->Length());
  EXPECT_EQ(out->GetNullCount(), tc.out.tensor->GetNullCount());
  EXPECT_EQ(out->Type(), tc.out.tensor->Type());
  // compare tensor content
  EXPECT_TRUE(
      out->ToArrowChunkedArray()->Equals(tc.out.tensor->ToArrowChunkedArray()));
}

/// ===========================
/// IfTest impl
/// ===========================

pb::ExecNode IfTest::MakeIfExecNode(const IfTestCase& tc) {
  test::ExecNodeBuilder builder(If::kOpType);

  builder.SetNodeName("if-test");
  // Add inputs
  auto cond = test::MakeTensorReference(tc.cond.name, tc.cond.tensor->Type(),
                                        tc.cond_status);
  builder.AddInput(If::kCond, std::vector<pb::Tensor>{cond});

  auto value_true = test::MakeTensorReference(
      tc.value_true.name, tc.value_true.tensor->Type(), tc.value_true_status);
  builder.AddInput(If::kValueTrue, std::vector<pb::Tensor>{value_true});

  auto value_false = test::MakeTensorReference(tc.value_false.name,
                                               tc.value_false.tensor->Type(),
                                               tc.value_false_status);
  builder.AddInput(If::kValueFalse, std::vector<pb::Tensor>{value_false});

  // Add outputs
  auto out = test::MakeTensorReference(tc.out.name, tc.out.tensor->Type(),
                                       tc.out_status);
  builder.AddOutput(If::kOut, std::vector<pb::Tensor>{out});

  return builder.Build();
}

void IfTest::FeedTensors(const std::vector<ExecContext*>& ctxs,
                         const std::vector<test::NamedTensor>& ts,
                         pb::TensorStatus status) {
  if (status == pb::TENSORSTATUS_PRIVATE) {
    test::FeedInputsAsPrivate(ctxs[0], ts);
  } else if (status == pb::TENSORSTATUS_SECRET) {
    test::FeedInputsAsSecret(ctxs, ts);
  } else {
    test::FeedInputsAsPublic(ctxs, ts);
  }
}

void IfTest::FeedInputs(const std::vector<ExecContext*>& ctxs,
                        const IfTestCase& tc) {
  FeedTensors(ctxs, {tc.cond}, tc.cond_status);
  FeedTensors(ctxs, {tc.value_true}, tc.value_true_status);
  FeedTensors(ctxs, {tc.value_false}, tc.value_false_status);
}

}  // namespace scql::engine::op
