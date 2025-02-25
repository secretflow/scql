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

#include "engine/operator/secret_join.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct SecretJoinTestCase {
  std::vector<test::NamedTensor> left_key;
  std::vector<test::NamedTensor> right_key;
  std::vector<test::NamedTensor> left_payload;
  std::vector<test::NamedTensor> right_payload;
  std::vector<test::NamedTensor> expect_left_output;
  std::vector<test::NamedTensor> expect_right_output;
};

class SecretJoinTest
    : public ::testing::TestWithParam<
          std::tuple<test::SpuRuntimeTestCase, SecretJoinTestCase>> {
 protected:
  static pb::ExecNode MakeSecretJoinExecNode(const SecretJoinTestCase& tc);

  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const SecretJoinTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    SecretJoinSecretTest, SecretJoinTest,
    testing::Combine(
        testing::Values(test::SpuRuntimeTestCase{spu::ProtocolKind::SEMI2K, 2},
                        test::SpuRuntimeTestCase{spu::ProtocolKind::SEMI2K, 3}),
        testing::Values(
            SecretJoinTestCase{
                .left_key = {test::NamedTensor(
                    "lk", TensorFrom(arrow::large_utf8(),
                                     R"json(["a", "b", "a", "c", "b"])json"))},
                .right_key = {test::NamedTensor(
                    "rk", TensorFrom(arrow::large_utf8(),
                                     R"json(["a", "a", "b", "d", "a"])json"))},
                .left_payload =
                    {test::NamedTensor(
                         "lp0",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["a", "b", "a", "c", "b"])json")),
                     test::NamedTensor(
                         "lp1",
                         TensorFrom(
                             arrow::large_utf8(),
                             R"json(["x1", "y1", "x2", "z1", "y2"])json"))},
                .right_payload =
                    {test::NamedTensor(
                         "rp0",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["a", "a", "b", "d", "a"])json")),
                     test::NamedTensor(
                         "rp1",
                         TensorFrom(
                             arrow::large_utf8(),
                             R"json(["r1", "r2", "s1", "t1", "r3"])json"))},
                .expect_left_output =
                    {test::NamedTensor(
                         "lp0",
                         TensorFrom(
                             arrow::large_utf8(),
                             R"json(["b", "b", "a", "a", "a", "a", "a", "a"])json")),
                     test::NamedTensor(
                         "lp1",
                         TensorFrom(
                             arrow::
                                 large_utf8(),
                             R"json(["y1", "y2", "x1", "x1", "x1", "x2", "x2", "x2"])json"))},
                .expect_right_output =
                    {test::NamedTensor(
                         "rp0",
                         TensorFrom(
                             arrow::large_utf8(),
                             R"json(["b", "b", "a", "a", "a", "a", "a", "a"])json")),
                     test::NamedTensor(
                         "rp1",
                         TensorFrom(
                             arrow::
                                 large_utf8(),
                             R"json(["s1", "s1", "r1", "r2", "r3", "r1", "r2", "r3"])json"))}},
            SecretJoinTestCase{
                .left_key = {test::NamedTensor(
                    "lk", TensorFrom(arrow::int64(), "[1, 2, 1, 2, 0]"))},
                .right_key = {test::NamedTensor(
                    "rk", TensorFrom(arrow::int64(), "[1, 2, 1, 2]"))},
                .left_payload =
                    {test::NamedTensor("lp0", TensorFrom(arrow::int64(),
                                                         "[1, 2, 1, 2, 0]")),
                     test::NamedTensor("lp1", TensorFrom(arrow::int64(),
                                                         "[0, 1, 2, 3, 4]"))},
                .right_payload =
                    {test::NamedTensor("rp0", TensorFrom(arrow::int64(),
                                                         "[1, 2, 1, 2]")),
                     test::NamedTensor("rp1", TensorFrom(arrow::int64(),
                                                         "[0, 1, 2, 3]"))},
                .expect_left_output =
                    {test::NamedTensor("lp0",
                                       TensorFrom(arrow::int64(),
                                                  "[1, 1, 1, 1, 2, 2, 2, 2]")),
                     test::NamedTensor("lp1",
                                       TensorFrom(arrow::int64(),
                                                  "[0, 0, 2, 2, 1, 1, 3, 3]"))},
                .expect_right_output =
                    {test::NamedTensor("rp0",
                                       TensorFrom(arrow::int64(),
                                                  "[1, 1, 1, 1, 2, 2, 2, 2]")),
                     test::NamedTensor(
                         "rp1", TensorFrom(arrow::int64(),
                                           "[0, 2, 0, 2, 1, 3, 1, 3]"))}},
            SecretJoinTestCase{
                .left_key =
                    {test::NamedTensor(
                         "lk0", TensorFrom(arrow::int64(), "[1, 1, 2, 2, 5]")),
                     test::NamedTensor(
                         "lk1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["a", "a", "b", "d", "a"])json"))},
                .right_key =
                    {test::NamedTensor("rk0", TensorFrom(arrow::int64(),
                                                         "[1, 1, 2, 2, 4, 6]")),
                     test::NamedTensor(
                         "rk1",
                         TensorFrom(
                             arrow::large_utf8(),
                             R"json(["a", "a", "b", "c", "e", "f"])json"))},
                .left_payload = {test::NamedTensor(
                    "lp1", TensorFrom(arrow::int64(), "[0, 1, 2, 3, 4]"))},
                .right_payload = {test::NamedTensor(
                    "rp1", TensorFrom(arrow::int64(), "[0, 1, 2, 3, 4, 5]"))},
                .expect_left_output = {test::NamedTensor(
                    "lo1", TensorFrom(arrow::int64(), "[0, 0, 1, 1, 2]"))},
                .expect_right_output = {test::NamedTensor(
                    "ro1", TensorFrom(arrow::int64(), "[0, 1, 0, 1, 2]"))}},
            SecretJoinTestCase{
                .left_key =
                    {test::NamedTensor(
                         "lk", TensorFrom(arrow::int64(), "[1, 1, 2, 2, 5]")),
                     test::NamedTensor(
                         "lk0", TensorFrom(arrow::float64(),
                                           "[1.0, 2.2, 3.3, 4.4, -5.5]")),
                     test::NamedTensor(
                         "lk1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["a", "a", "b", "d", "a"])json"))},
                .right_key =
                    {test::NamedTensor("rk", TensorFrom(arrow::int64(),
                                                        "[1, 1, 2, 2, 4, 6]")),
                     test::NamedTensor(
                         "rk0",
                         TensorFrom(arrow::float64(),
                                    "[-1.0, -2.2, -3.3, -4.4, -5.5, 6.6]")),
                     test::NamedTensor(
                         "rk1",
                         TensorFrom(
                             arrow::large_utf8(),
                             R"json(["a", "a", "b", "c", "e", "f"])json"))},
                .left_payload = {test::NamedTensor(
                    "lp1", TensorFrom(arrow::int64(), "[0, 1, 2, 3, 4]"))},
                .right_payload = {test::NamedTensor(
                    "rp1", TensorFrom(arrow::int64(), "[0, 1, 2, 3, 4, 5]"))},
                .expect_left_output = {test::NamedTensor(
                    "lo1", TensorFrom(arrow::int64(), "[]"))},
                .expect_right_output = {test::NamedTensor(
                    "ro1", TensorFrom(arrow::int64(), "[]"))}},
            SecretJoinTestCase{.left_key = {test::NamedTensor(
                                   "lk0", TensorFrom(arrow::int64(), "[]"))},
                               .right_key = {test::NamedTensor(
                                   "rk0", TensorFrom(arrow::int64(), "[]"))},
                               .left_payload = {test::NamedTensor(
                                   "lp1", TensorFrom(arrow::int64(), "[]"))},
                               .right_payload = {test::NamedTensor(
                                   "rp1", TensorFrom(arrow::int64(), "[]"))},
                               .expect_left_output = {test::NamedTensor(
                                   "lo1", TensorFrom(arrow::int64(), "[]"))},
                               .expect_right_output = {test::NamedTensor(
                                   "ro1", TensorFrom(arrow::int64(), "[]"))}})),
    TestParamNameGenerator(SecretJoinTest));

TEST_P(SecretJoinTest, works) {
  // Given
  auto parm = GetParam();
  auto tc = std::get<1>(parm);
  auto node = MakeSecretJoinExecNode(tc);
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

  // When
  EXPECT_NO_THROW(test::RunAsync<SecretJoin>(ctx_ptrs));

  // Then check outputs in alice: reveal all secret output to
  // alice

  auto expect_output = tc.expect_left_output;
  expect_output.insert(expect_output.end(), tc.expect_right_output.begin(),
                       tc.expect_right_output.end());
  for (const auto& expect_t : expect_output) {
    TensorPtr out;
    EXPECT_NO_THROW(out = test::RevealSecret(ctx_ptrs, expect_t.name));
    // convert hash to string for string tensor in spu
    if (expect_t.tensor->Type() == pb::PrimitiveDataType::STRING) {
      out = ctx_ptrs[0]->GetSession()->HashToString(*out);
    }
    EXPECT_TRUE(out != nullptr);
    // compare tensor content
    EXPECT_TRUE(out->ToArrowChunkedArray()->Equals(
        expect_t.tensor->ToArrowChunkedArray()))
        << "actual output = " << out->ToArrowChunkedArray()->ToString()
        << ", expect output = "
        << expect_t.tensor->ToArrowChunkedArray()->ToString();
  }
}

/// ===========================
/// SecretJoinTest impl
/// ===========================

pb::ExecNode SecretJoinTest::MakeSecretJoinExecNode(
    const SecretJoinTestCase& tc) {
  test::ExecNodeBuilder builder(SecretJoin::kOpType);

  builder.SetNodeName("secret-join-test");
  // Add inputs
  {
    std::vector<pb::Tensor> input_datas;
    for (const auto& named_tensor : tc.left_key) {
      auto data = test::MakeSecretTensorReference(named_tensor.name,
                                                  named_tensor.tensor->Type());
      input_datas.push_back(std::move(data));
    }
    builder.AddInput(SecretJoin::kLeftKey, input_datas);
  }
  {
    std::vector<pb::Tensor> input_datas;
    for (const auto& named_tensor : tc.right_key) {
      auto data = test::MakeSecretTensorReference(named_tensor.name,
                                                  named_tensor.tensor->Type());
      input_datas.push_back(std::move(data));
    }
    builder.AddInput(SecretJoin::kRightKey, input_datas);
  }
  {
    std::vector<pb::Tensor> input_datas;
    for (const auto& named_tensor : tc.left_payload) {
      auto data = test::MakeSecretTensorReference(named_tensor.name,
                                                  named_tensor.tensor->Type());
      input_datas.push_back(std::move(data));
    }
    builder.AddInput(SecretJoin::kLeftPayload, input_datas);
  }
  {
    std::vector<pb::Tensor> input_datas;
    for (const auto& named_tensor : tc.right_payload) {
      auto data = test::MakeSecretTensorReference(named_tensor.name,
                                                  named_tensor.tensor->Type());
      input_datas.push_back(std::move(data));
    }
    builder.AddInput(SecretJoin::kRightPayload, input_datas);
  }

  // Add outputs
  {
    std::vector<pb::Tensor> outputs;
    for (const auto& named_tensor : tc.expect_left_output) {
      auto data = test::MakeSecretTensorReference(named_tensor.name,
                                                  named_tensor.tensor->Type());
      outputs.push_back(std::move(data));
    }
    builder.AddOutput(SecretJoin::kOutLeft, outputs);
  }
  {
    std::vector<pb::Tensor> outputs;
    for (const auto& named_tensor : tc.expect_right_output) {
      auto data = test::MakeSecretTensorReference(named_tensor.name,
                                                  named_tensor.tensor->Type());
      outputs.push_back(std::move(data));
    }
    builder.AddOutput(SecretJoin::kOutRight, outputs);
  }

  return builder.Build();
}

void SecretJoinTest::FeedInputs(const std::vector<ExecContext*>& ctxs,
                                const SecretJoinTestCase& tc) {
  test::FeedInputsAsSecret(ctxs, tc.left_key);
  test::FeedInputsAsSecret(ctxs, tc.right_key);
  test::FeedInputsAsSecret(ctxs, tc.left_payload);
  test::FeedInputsAsSecret(ctxs, tc.right_payload);
}

}  // namespace scql::engine::op
