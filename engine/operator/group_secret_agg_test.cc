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

#include "engine/operator/group_secret_agg.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct GroupSecretAggTestCase {
  test::NamedTensor group_num;
  test::NamedTensor group_id;
  test::NamedTensor input;
  test::NamedTensor output;
  std::string op_type;
};

class GroupSecretAggTest
    : public testing::TestWithParam<
          std::tuple<test::SpuRuntimeTestCase, GroupSecretAggTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const GroupSecretAggTestCase& tc) {
    test::ExecNodeBuilder builder(tc.op_type);

    builder.SetNodeName("group-secret-test");
    {
      auto t = test::MakeTensorReference(tc.group_num.name,
                                         tc.group_num.tensor->Type(),
                                         pb::TensorStatus::TENSORSTATUS_PUBLIC);
      builder.AddInput("GroupNum", {t});
    }
    {
      auto t = test::MakeSecretTensorReference(tc.group_id.name,
                                               tc.group_id.tensor->Type());
      builder.AddInput("GroupId", {t});
    }
    {
      auto t = test::MakeSecretTensorReference(tc.input.name,
                                               tc.input.tensor->Type());
      builder.AddInput("In", {t});
    }

    {
      auto t = test::MakeSecretTensorReference(tc.output.name,
                                               tc.output.tensor->Type());
      builder.AddOutput("Out", {t});
    }

    return builder.Build();
  }

  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const GroupSecretAggTestCase& tc) {
    test::FeedInputsAsSecret(ctxs, {tc.group_id, tc.input});
    test::FeedInputsAsPublic(ctxs, {tc.group_num});
  }
};

INSTANTIATE_TEST_SUITE_P(
    GroupSecretSumBatchTest, GroupSecretAggTest,
    testing::Combine(
        test::SpuTestValuesMultiPCEnableColocated,
        testing::Values(
            GroupSecretAggTestCase{
                .group_num = test::NamedTensor(
                    "num", TensorFrom(arrow::uint32(), "[3]")),
                .group_id = test::NamedTensor(
                    "id", TensorFrom(arrow::int32(), "[0, 1, 2, 1, 0]")),
                .input = test::NamedTensor(
                    "val", TensorFrom(arrow::int64(), "[1, 2, 3, 20, 10]")),
                .output = test::NamedTensor("agg", TensorFrom(arrow::int64(),
                                                              "[11, 22, 3]")),
                .op_type = GroupSecretSum::kOpType},
            GroupSecretAggTestCase{
                .group_num = test::NamedTensor(
                    "num", TensorFrom(arrow::uint32(), "[3]")),
                .group_id = test::NamedTensor(
                    "id", TensorFrom(arrow::int32(), "[0, 1, 2, 1, 0]")),
                .input = test::NamedTensor(
                    "val", TensorFrom(arrow::float64(),
                                      "[-1, 2.2, 3.333, -20.2, 2.2]")),
                .output = test::NamedTensor(
                    "agg", TensorFrom(arrow::float64(), "[1.2, -18, 3.333]")),
                .op_type = GroupSecretSum::kOpType},
            GroupSecretAggTestCase{
                .group_num = test::NamedTensor(
                    "num", TensorFrom(arrow::uint32(), "[0]")),
                .group_id = test::NamedTensor("id",
                                              TensorFrom(arrow::int32(), "[]")),
                .input = test::NamedTensor("val",
                                           TensorFrom(arrow::int64(), "[]")),
                .output = test::NamedTensor("agg",
                                            TensorFrom(arrow::int64(), "[]")),
                .op_type = GroupSecretSum::kOpType})),
    TestParamNameGenerator(GroupSecretAggTest));

INSTANTIATE_TEST_SUITE_P(
    GroupSecretAvgBatchTest, GroupSecretAggTest,
    testing::Combine(
        test::SpuTestValuesMultiPCEnableColocated,
        testing::Values(
            GroupSecretAggTestCase{
                .group_num = test::NamedTensor(
                    "num", TensorFrom(arrow::uint32(), "[3]")),
                .group_id = test::NamedTensor(
                    "id", TensorFrom(arrow::int32(), "[0, 1, 2, 1, 0]")),
                .input = test::NamedTensor(
                    "val", TensorFrom(arrow::int64(), "[1, 2, 3, 20, 10]")),
                .output = test::NamedTensor("agg", TensorFrom(arrow::float32(),
                                                              "[5.5, 11, 3]")),
                .op_type = GroupSecretAvg::kOpType},
            GroupSecretAggTestCase{
                .group_num = test::NamedTensor(
                    "num", TensorFrom(arrow::uint32(), "[3]")),
                .group_id = test::NamedTensor(
                    "id", TensorFrom(arrow::int32(), "[0, 1, 2, 1, 0]")),
                .input = test::NamedTensor(
                    "val", TensorFrom(arrow::float64(),
                                      "[-1, 2.2, 3.333, -20.2, 2.2]")),
                .output = test::NamedTensor(
                    "agg", TensorFrom(arrow::float64(), "[0.6, -9, 3.333]")),
                .op_type = GroupSecretAvg::kOpType},
            GroupSecretAggTestCase{
                .group_num = test::NamedTensor(
                    "num", TensorFrom(arrow::uint32(), "[0]")),
                .group_id = test::NamedTensor("id",
                                              TensorFrom(arrow::int32(), "[]")),
                .input = test::NamedTensor("val",
                                           TensorFrom(arrow::int64(), "[]")),
                .output = test::NamedTensor("agg",
                                            TensorFrom(arrow::float64(), "[]")),
                .op_type = GroupSecretAvg::kOpType})),
    TestParamNameGenerator(GroupSecretAggTest));

TEST_P(GroupSecretAggTest, Works) {
  auto parm = GetParam();
  auto tc = std::get<1>(parm);

  auto node = MakeExecNode(tc);

  auto sessions = test::MakeMultiPCSession(std::get<0>(parm));

  std::vector<ExecContext> exec_ctxs;
  exec_ctxs.reserve(sessions.size());
  for (auto& session : sessions) {
    exec_ctxs.emplace_back(node, session.get());
  }

  // feed inputs
  std::vector<ExecContext*> ctx_ptrs;
  ctx_ptrs.reserve(exec_ctxs.size());
  for (auto& exec_ctx : exec_ctxs) {
    ctx_ptrs.emplace_back(&exec_ctx);
  }
  FeedInputs(ctx_ptrs, tc);

  if (tc.op_type == GroupSecretSum::kOpType) {
    ASSERT_NO_THROW(test::RunAsync<GroupSecretSum>(ctx_ptrs));
  } else if (tc.op_type == GroupSecretAvg::kOpType) {
    ASSERT_NO_THROW(test::RunAsync<GroupSecretAvg>(ctx_ptrs));
  } else {
    ASSERT_TRUE(false) << "unsupported op type: " << tc.op_type;
  }

  std::vector<TensorPtr> outs;
  EXPECT_NO_THROW({
    auto t = test::RevealSecret(ctx_ptrs, tc.output.name);
    ASSERT_TRUE(t != nullptr);
    auto out_arr = t->ToArrowChunkedArray();
    auto expect_arr = tc.output.tensor->ToArrowChunkedArray();
    arrow::EqualOptions eqOption;
    EXPECT_TRUE(out_arr->ApproxEquals(*expect_arr, eqOption.atol(0.001)))
        << "expect type = " << expect_arr->type()->ToString()
        << ", got type = " << out_arr->type()->ToString()
        << "\nexpect result = " << expect_arr->ToString()
        << "\nbut actual got result = " << out_arr->ToString();
  });
}

}  // namespace scql::engine::op
