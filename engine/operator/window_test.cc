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
#include "engine/operator/window.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {
struct WindowTestCase {
  std::string op_type;
  std::vector<test::NamedTensor> inputs;
  test::NamedTensor partition_id;
  test::NamedTensor partition_num;
  std::vector<test::NamedTensor> outputs;
  std::vector<std::string> reverse;
  bool ok = true;
};

class WindowTest : public testing::TestWithParam<WindowTestCase> {
 protected:
  void SetUp() override {}

  static pb::ExecNode MakeExecNode(const WindowTestCase& tc);
};

std::string MakeLargeIncrementTensorInput(const size_t size) {
  std::string tensor_str = "[";
  for (size_t i = 0; i < size; i++) {
    tensor_str += std::to_string(i + 1);
    if (i != size - 1) {
      tensor_str += ",";
    }
  }

  tensor_str += "]";
  return tensor_str;
}

std::string MakeAllZeroTensor(const size_t size) {
  std::string tensor_str = "[";
  for (size_t i = 0; i < size; i++) {
    tensor_str += "0";
    if (i != size - 1) {
      tensor_str += ",";
    }
  }

  tensor_str += "]";
  return tensor_str;
}

INSTANTIATE_TEST_SUITE_P(
    WindowBatchTest, WindowTest,
    testing::Values(
        WindowTestCase{
            .op_type = RowNumber::kOpType,
            .inputs = {test::NamedTensor("in_a",
                                         TensorFrom(arrow::int64(), "[]"))},
            .partition_id = test::NamedTensor(
                "partition_id", TensorFrom(arrow::uint32(), "[]")),
            .partition_num = test::NamedTensor(
                "partition_num", TensorFrom(arrow::uint32(), "[0]")),
            .outputs = {test::NamedTensor("out",
                                          TensorFrom(arrow::int64(), "[]"))},
            .reverse = {"0"}},
        WindowTestCase{
            .op_type = RowNumber::kOpType,
            .inputs = {test::NamedTensor(
                "in_a", TensorFrom(arrow::int64(), "[5,5,5,5,6,5,6,5]"))},
            .partition_id = test::NamedTensor("partition_id",
                                              TensorFrom(arrow::uint32(),
                                                         "[0,1,1,1,0,0,1,0]")),
            .partition_num = test::NamedTensor(
                "partition_num", TensorFrom(arrow::uint32(), "[2]")),
            .outputs = {test::NamedTensor(
                "out", TensorFrom(arrow::int64(), "[1,1,2,3,4,2,4,3]"))},
            .reverse = {"0"}},
        WindowTestCase{
            .op_type = RowNumber::kOpType,
            .inputs = {test::NamedTensor(
                "in_a",
                TensorFrom(arrow::int64(), "[10,20,30,40,50,60,70,80]"))},
            .partition_id = test::NamedTensor("partition_id",
                                              TensorFrom(arrow::uint32(),
                                                         "[0,0,1,1,0,1,2,2]")),
            .partition_num = test::NamedTensor(
                "partition_num", TensorFrom(arrow::uint32(), "[3]")),
            .outputs = {test::NamedTensor(
                "out", TensorFrom(arrow::int64(), "[1,2,1,2,3,3,1,2]"))},
            .reverse = {"0"}},
        WindowTestCase{
            .op_type = RowNumber::kOpType,
            .inputs = {test::NamedTensor(
                           "in_a", TensorFrom(arrow::int64(),
                                              "[10,20,30,40,50,60,70,80]")),
                       test::NamedTensor("in_b",
                                         TensorFrom(arrow::int32(),
                                                    "[1,2,3,4,5,6,7,8]"))},
            .partition_id = test::NamedTensor("partition_id",
                                              TensorFrom(arrow::uint32(),
                                                         "[0,0,1,1,0,1,2,2]")),
            .partition_num = test::NamedTensor(
                "partition_num", TensorFrom(arrow::uint32(), "[3]")),
            .outputs = {test::NamedTensor(
                "out", TensorFrom(arrow::int64(), "[1,2,1,2,3,3,1,2]"))},
            .reverse = {"0", "0"}},
        WindowTestCase{
            .op_type = RowNumber::kOpType,
            .inputs =
                {test::NamedTensor("in_a", TensorFrom(arrow::int64(),
                                                      "[1,2,3,4,5,6,7,8,9]")),
                 test::NamedTensor("in_b", TensorFrom(arrow::int32(),
                                                      "[1,2,3,4,5,6,7,8,9]"))},
            .partition_id = test::NamedTensor(
                "partition_id",
                TensorFrom(arrow::uint32(), "[0,0,0,0,0,1,1,1,1]")),
            .partition_num = test::NamedTensor(
                "partition_num", TensorFrom(arrow::uint32(), "[2]")),
            .outputs = {test::NamedTensor(
                "out", TensorFrom(arrow::int64(), "[5,4,3,2,1,4,3,2,1]"))},
            .reverse = {"1", "0"}},
        WindowTestCase{
            .op_type = RowNumber::kOpType,
            .inputs = {test::NamedTensor(
                "in_a", TensorFrom(arrow::int64(),
                                   MakeLargeIncrementTensorInput(10000)))},
            .partition_id = test::NamedTensor(
                "partition_id",
                TensorFrom(arrow::uint32(), MakeAllZeroTensor(10000))),
            .partition_num = test::NamedTensor(
                "partition_num", TensorFrom(arrow::uint32(), "[1]")),
            .outputs = {test::NamedTensor(
                "out", TensorFrom(arrow::int64(),
                                  MakeLargeIncrementTensorInput(10000)))},
            .reverse = {"0"}},
        WindowTestCase{
            .op_type = RowNumber::kOpType,
            .inputs = {test::NamedTensor(
                "in_a",
                TensorFrom(arrow::int64(),
                           MakeLargeIncrementTensorInput(1024 * 1024)))},
            .partition_id = test::NamedTensor(
                "partition_id",
                TensorFrom(arrow::uint32(), MakeAllZeroTensor(1024 * 1024))),
            .partition_num = test::NamedTensor(
                "partition_num", TensorFrom(arrow::uint32(), "[1]")),
            .outputs = {test::NamedTensor(
                "out", TensorFrom(arrow::int64(),
                                  MakeLargeIncrementTensorInput(1024 * 1024)))},
            .reverse = {"0"}}));

TEST_P(WindowTest, work) {
  auto tc = GetParam();
  auto node = MakeExecNode(tc);
  auto session = test::Make1PCSession();

  ExecContext ctx(node, session.get());

  test::FeedInputsAsPrivate(&ctx, tc.inputs);
  test::FeedInputsAsPrivate(&ctx, {tc.partition_id, tc.partition_num});

  RowNumber op;

  if (tc.ok) {
    EXPECT_NO_THROW(op.Run(&ctx));
  } else {
    EXPECT_ANY_THROW(op.Run(&ctx));
  }

  SPDLOG_INFO("start to compare result");
  auto actual_output = ctx.GetTensorTable()->GetTensor(tc.outputs[0].name);

  auto actual_arr = actual_output->ToArrowChunkedArray();
  auto expect_arr = tc.outputs[0].tensor->ToArrowChunkedArray();

  EXPECT_TRUE(actual_arr->ApproxEquals(*expect_arr))
      << "\nexpect result = " << expect_arr->ToString()
      << "\nactual result = " << actual_arr->ToString();
}

pb::ExecNode WindowTest::MakeExecNode(const WindowTestCase& tc) {
  test::ExecNodeBuilder builder(tc.op_type);
  builder.AddStringsAttr(RowNumber::kReverseAttr, tc.reverse);
  builder.SetNodeName("plaintext-window-test");

  std::vector<pb::Tensor> inputs;
  for (const auto& named_tensor : tc.inputs) {
    pb::Tensor t = test::MakePrivateTensorReference(
        named_tensor.name, named_tensor.tensor->Type());
    inputs.push_back(std::move(t));
  }
  builder.AddInput(RankWindowBase::kIn, inputs);

  pb::Tensor partition_id = test::MakePrivateTensorReference(
      tc.partition_id.name, tc.partition_id.tensor->Type());
  builder.AddInput(RankWindowBase::kPartitionId, {partition_id});
  pb::Tensor partition_num = test::MakePrivateTensorReference(
      tc.partition_num.name, tc.partition_num.tensor->Type());
  builder.AddInput(RankWindowBase::kPartitionNum, {partition_num});

  std::vector<pb::Tensor> outputs;
  for (const auto& named_tensor : tc.outputs) {
    pb::Tensor t = test::MakePrivateTensorReference(
        named_tensor.name, named_tensor.tensor->Type());
    outputs.push_back(std::move(t));
  }

  builder.AddOutput(RankWindowBase::kOut, outputs);

  return builder.Build();
}
}  // namespace scql::engine::op