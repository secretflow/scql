// Copyright 2024 Ant Group Co., Ltd.
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
#include "engine/operator/replicate.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {
struct ReplicateTestCase {
  std::string op = Replicate::kOpType;
  std::vector<test::NamedTensor> left_inputs;
  std::vector<test::NamedTensor> right_inputs;
  std::vector<test::NamedTensor> left_outputs;
  std::vector<test::NamedTensor> right_outputs;
  int world_size = 1;
};

class ReplicateTest
    : public ::testing::TestWithParam<
          std::tuple<test::SpuRuntimeTestCase, ReplicateTestCase>> {
 protected:
  void SetUp() override {}
  static pb::ExecNode MakeExecNode(const ReplicateTestCase& tc,
                                   const std::string& name);
  static void FeedInputs(const std::vector<ExecContext*>& ctx,
                         const ReplicateTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    ReplicateTestSuite, ReplicateTest,
    testing::Combine(
        test::SpuTestValues2PC,
        testing::Values(
            ReplicateTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int32(), "[1,2,3]"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::float32(), "[100.0, 200.0]"))},
                .left_outputs = {test::NamedTensor(
                    "output_x", TensorFrom(arrow::int32(), "[1,2,3,1,2,3]"))},
                .right_outputs = {test::NamedTensor(
                    "output_y",
                    TensorFrom(arrow::float32(),
                               "[100.0, 100.0, 100.0, 200.0, 200.0, 200.0]"))},
                .world_size = 2},
            ReplicateTestCase{
                .left_inputs = {test::NamedTensor(
                                    "x", TensorFrom(arrow::int32(), "[1,2,3]")),
                                test::NamedTensor("x_1",
                                                  TensorFrom(arrow::int32(),
                                                             "[4,5,6]"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::float32(), "[100.0, 200.0]"))},
                .left_outputs = {test::NamedTensor("output_x",
                                                   TensorFrom(arrow::int32(),
                                                              "[1,2,3,1,2,3]")),
                                 test::NamedTensor(
                                     "output_x_1",
                                     TensorFrom(arrow::int32(),
                                                "[4,5,6,4,5,6]"))},
                .right_outputs = {test::NamedTensor(
                    "output_y",
                    TensorFrom(arrow::float32(),
                               "[100.0, 100.0, 100.0, 200.0, 200.0, 200.0]"))},
                .world_size = 2},
            ReplicateTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int32(), "[1,2,3]"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::float32(), "[]"))},
                .left_outputs = {test::NamedTensor(
                    "output_x", TensorFrom(arrow::int32(), "[]"))},
                .right_outputs = {test::NamedTensor(
                    "output_y", TensorFrom(arrow::float32(), "[]"))},
                .world_size = 2},
            ReplicateTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int32(), "[1,2,3]"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::float32(), "[100.0, 200.0]"))},
                .left_outputs = {test::NamedTensor(
                    "output_x", TensorFrom(arrow::int32(), "[1,2,3,1,2,3]"))},
                .right_outputs = {test::NamedTensor(
                    "output_y",
                    TensorFrom(arrow::float32(),
                               "[100.0, 100.0, 100.0, 200.0, 200.0, 200.0]"))}},
            ReplicateTestCase{
                .left_inputs = {test::NamedTensor(
                                    "x", TensorFrom(arrow::int32(), "[1,2,3]")),
                                test::NamedTensor("x_1",
                                                  TensorFrom(arrow::int32(),
                                                             "[4,5,6]"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(), "[\"a\", \"b\"]"))},
                .left_outputs = {test::NamedTensor("output_x",
                                                   TensorFrom(arrow::int32(),
                                                              "[1,2,3,1,2,3]")),
                                 test::NamedTensor(
                                     "output_x_1",
                                     TensorFrom(arrow::int32(),
                                                "[4,5,6,4,5,6]"))},
                .right_outputs = {test::NamedTensor(
                    "output_y",
                    TensorFrom(arrow::large_utf8(),
                               "[\"a\", \"a\", \"a\", \"b\", \"b\", \"b\"]"))}},
            ReplicateTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::int32(), "[1,2,3]"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::float32(), "[1]"))},
                .left_outputs = {test::NamedTensor(
                    "output_x", TensorFrom(arrow::int32(), "[1,2,3]"))},
                .right_outputs = {test::NamedTensor(
                    "output_y", TensorFrom(arrow::float32(), "[1,1,1]"))}})));

TEST_P(ReplicateTest, Works) {
  auto param = GetParam();
  auto tc = std::get<1>(param);
  auto node = MakeExecNode(tc, tc.op);
  std::vector<std::shared_ptr<scql::engine::Session>> sessions;
  if (tc.world_size > 1) {
    sessions = test::MakeMultiPCSession(std::get<0>(param));
  } else {
    sessions.push_back(test::Make1PCSession());
  }

  std::vector<ExecContext> exec_ctx;
  exec_ctx.reserve(sessions.size());
  for (auto& session : sessions) {
    exec_ctx.emplace_back(node, session.get());
  }

  std::vector<ExecContext*> ctx_ptrs;
  ctx_ptrs.reserve(exec_ctx.size());
  for (auto& i : exec_ctx) {
    ctx_ptrs.emplace_back(&i);
  }
  FeedInputs(ctx_ptrs, tc);
  EXPECT_NO_THROW(test::RunAsync<Replicate>(ctx_ptrs));

  if (tc.world_size == 1) {
    SPDLOG_INFO("world size = 1");
    ExecContext* alice_ctx = nullptr;
    for (auto& c : ctx_ptrs) {
      if (c->GetSession()->SelfPartyCode() == test::kPartyAlice) {
        alice_ctx = c;
        break;
      }
    }
    EXPECT_NE(alice_ctx, nullptr);
    for (auto& left : tc.left_outputs) {
      TensorPtr left_output = alice_ctx->GetTensorTable()->GetTensor(left.name);
      auto out_arr = left_output->ToArrowChunkedArray();
      auto expect_arr = left.tensor->ToArrowChunkedArray();
      arrow::EqualOptions eq;
      EXPECT_TRUE(out_arr->ApproxEquals(*expect_arr, eq.atol(0.001)))
          << "alice left output:"
          << "\nactual: " << out_arr->ToString()
          << "\nexpect: " << expect_arr->ToString();
    }

    for (auto& right : tc.right_outputs) {
      TensorPtr right_output =
          alice_ctx->GetTensorTable()->GetTensor(right.name);
      auto out_arr = right_output->ToArrowChunkedArray();
      auto expect_arr = right.tensor->ToArrowChunkedArray();
      arrow::EqualOptions eq;
      EXPECT_TRUE(out_arr->ApproxEquals(*expect_arr, eq.atol(0.001)))
          << "alice right output:"
          << "\nactual: " << out_arr->ToString()
          << "\nexpect: " << expect_arr->ToString();
    }
  } else {
    SPDLOG_INFO("world size = 2");
    ExecContext* alice_ctx;
    ExecContext* bob_ctx;
    for (auto& ctx : ctx_ptrs) {
      if (ctx->GetSession()->SelfPartyCode() == test::kPartyAlice) {
        alice_ctx = ctx;
      } else if (ctx->GetSession()->SelfPartyCode() == test::kPartyBob) {
        bob_ctx = ctx;
      }
    }

    EXPECT_NE(alice_ctx, nullptr);
    EXPECT_NE(bob_ctx, nullptr);
    for (auto& left : tc.left_outputs) {
      TensorPtr left_output = alice_ctx->GetTensorTable()->GetTensor(left.name);
      auto out_arr = left_output->ToArrowChunkedArray();
      auto expect_arr = left.tensor->ToArrowChunkedArray();
      arrow::EqualOptions eq;
      EXPECT_TRUE(out_arr->ApproxEquals(*expect_arr, eq.atol(0.001)))
          << "alice left output:"
          << "\nactual: " << out_arr->ToString()
          << "\nexpect: " << expect_arr->ToString();
    }

    for (auto& right : tc.right_outputs) {
      TensorPtr right_output = bob_ctx->GetTensorTable()->GetTensor(right.name);
      auto out_arr = right_output->ToArrowChunkedArray();
      auto expect_arr = right.tensor->ToArrowChunkedArray();
      arrow::EqualOptions eq;
      EXPECT_TRUE(out_arr->ApproxEquals(*expect_arr, eq.atol(0.001)))
          << "bob right output:"
          << "\nactual: " << out_arr->ToString()
          << "\nexpect: " << expect_arr->ToString();
    }
  }
}

pb::ExecNode ReplicateTest::MakeExecNode(const ReplicateTestCase& tc,
                                         const std::string& name) {
  test::ExecNodeBuilder builder(name);
  builder.SetNodeName(name + "-test");

  {
    std::vector<pb::Tensor> left_tensors;
    left_tensors.reserve(tc.left_inputs.size());
    for (const auto& left : tc.left_inputs) {
      left_tensors.push_back(test::MakeTensorReference(
          left.name, left.tensor->Type(), pb::TENSORSTATUS_PRIVATE));
    }

    builder.AddInput(Replicate::kLeft, left_tensors);
  }

  {
    std::vector<pb::Tensor> right_tensors;
    right_tensors.reserve(tc.right_inputs.size());
    for (const auto& right : tc.right_inputs) {
      right_tensors.emplace_back(test::MakeTensorReference(
          right.name, right.tensor->Type(), pb::TENSORSTATUS_PRIVATE));
    }
    builder.AddInput(Replicate::kRight, right_tensors);
  }

  {
    std::vector<pb::Tensor> left_output_tensors;
    left_output_tensors.reserve(tc.left_outputs.size());
    for (const auto& left : tc.left_outputs) {
      left_output_tensors.push_back(test::MakeTensorReference(
          left.name, left.tensor->Type(), pb::TENSORSTATUS_PRIVATE));
    }

    builder.AddOutput(Replicate::kLeftOut, left_output_tensors);
  }

  {
    std::vector<pb::Tensor> right_output_tensors;
    right_output_tensors.reserve(tc.right_outputs.size());
    for (const auto& right : tc.right_outputs) {
      right_output_tensors.push_back(test::MakeTensorReference(
          right.name, right.tensor->Type(), pb::TENSORSTATUS_PRIVATE));
    }

    builder.AddOutput(Replicate::kRightOut, right_output_tensors);
  }

  if (tc.world_size == 1) {
    builder.AddStringsAttr(Replicate::kInputPartyCodesAttr,
                           {test::kPartyAlice, test::kPartyAlice});
  } else {
    builder.AddStringsAttr(Replicate::kInputPartyCodesAttr,
                           {test::kPartyAlice, test::kPartyBob});
  }

  return builder.Build();
}

void ReplicateTest::FeedInputs(const std::vector<ExecContext*>& ctxs,
                               const ReplicateTestCase& tc) {
  if (ctxs.size() == 1) {
    test::FeedInputsAsPrivate(ctxs[0], {tc.left_inputs});
    test::FeedInputsAsPrivate(ctxs[0], {tc.right_inputs});
  } else {
    for (const auto& ctx : ctxs) {
      if (ctx->GetSession()->SelfPartyCode() == test::kPartyAlice) {
        test::FeedInputsAsPrivate(ctx, {tc.left_inputs});
      } else if (ctx->GetSession()->SelfPartyCode() == test::kPartyBob) {
        test::FeedInputsAsPrivate(ctx, {tc.right_inputs});
      }
    }
  }
}
}  // namespace scql::engine::op