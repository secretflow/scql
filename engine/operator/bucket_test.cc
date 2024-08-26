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

#include "engine/operator/bucket.h"

#include "arrow/array/concatenate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/type.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct BucketTestCase {
  std::vector<test::NamedTensor> keys;
  std::vector<test::NamedTensor> datas;
  std::vector<std::string> output_names;
};

test::NamedTensor MockInt32Tensor(const std::string& name,
                                  const size_t tensor_length) {
  std::vector<test::NamedTensor> tensors;
  std::string tensor_str = "[";
  for (size_t i = 0; i < tensor_length; i++) {
    tensor_str += std::to_string(i);
    if (i < tensor_length - 1) {
      tensor_str += ",";
    }
  }
  tensor_str += "]";
  return test::NamedTensor(name, TensorFrom(arrow::int32(), tensor_str));
};

test::NamedTensor MockStringTensor(const std::string& name,
                                   const size_t tensor_length) {
  BucketTestCase test_case;
  std::string tensor_str = "[";
  for (size_t i = 0; i < tensor_length; i++) {
    tensor_str += "\"" + std::to_string(i) + "\"";
    if (i < tensor_length - 1) {
      tensor_str += ",";
    }
  }
  tensor_str += "]";
  return test::NamedTensor(name, TensorFrom(arrow::large_utf8(), tensor_str));
};

BucketTestCase MockTestCase(const size_t tensor_length) {
  BucketTestCase tc;
  tc.keys.push_back(MockStringTensor("key", tensor_length));
  tc.datas.push_back(MockInt32Tensor("data1", tensor_length));
  tc.datas.push_back(MockStringTensor("data2", tensor_length));
  tc.output_names.push_back("data1_out");
  tc.output_names.push_back("data2_out");
  return tc;
}

class BucketTest : public ::testing::TestWithParam<BucketTestCase> {
 protected:
  static pb::ExecNode MakeBucketExecNode(const BucketTestCase& tc);

  static void FeedInputs(ExecContext* ctx, const BucketTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    BucketPrivateTest, BucketTest,
    testing::Values(
        BucketTestCase{
            .keys = {test::NamedTensor(
                "k1", TensorFrom(arrow::large_utf8(),
                                 R"json(["A","C","B","A"])json"))},
            .datas = {test::NamedTensor(
                          "x1", TensorFrom(arrow::float64(),
                                           "[-3.1415, 0.1, 99.999, null]")),
                      test::NamedTensor("x2", TensorFrom(arrow::boolean(),
                                                         "[1,0,0,1]"))},
            .output_names = {"x1_out", "x2_out"}},
        BucketTestCase{
            .keys = {test::NamedTensor("k1", TensorFrom(arrow::int64(), "[]"))},
            .datas = {test::NamedTensor("x1",
                                        TensorFrom(arrow::int64(), "[]"))},
            .output_names = {"x1_out"}},
        MockTestCase(100), MockTestCase(1100), MockTestCase(10000)));

std::shared_ptr<arrow::Array> SortChunkedArray(
    std::shared_ptr<arrow::ChunkedArray> arr) {
  auto indices = arrow::compute::SortIndices(
                     *arr.get(), arrow::compute::SortOrder::Ascending)
                     .ValueOrDie();
  auto concat_array = arrow::Concatenate(arr->chunks()).ValueOrDie();
  return arrow::compute::Take(*concat_array.get(), *indices.get()).ValueOrDie();
}

TEST_P(BucketTest, works) {
  // Given
  auto tc = GetParam();
  auto node = MakeBucketExecNode(tc);
  auto session = test::Make1PCSession();
  auto options = session->GetStreamingOptions();
  options.batch_row_num = 1000;
  options.streaming_row_num_threshold = 1000;
  session->SetStreamingOptions(options);
  ExecContext ctx(node, session.get());
  // feed inputs, test copy from alice to bob.
  FeedInputs(&ctx, tc);
  Bucket op;
  // When
  EXPECT_NO_THROW({ op.Run(&ctx); });

  // Then check bob output
  auto tensor_table = ctx.GetTensorTable();
  for (size_t i = 0; i < tc.output_names.size(); ++i) {
    auto in_arr = tc.datas[i].tensor->ToArrowChunkedArray();
    auto out = tensor_table->GetTensor(tc.output_names[i]);
    ASSERT_TRUE(out);
    // compare tensor content
    auto sorted_in_arr = SortChunkedArray(in_arr);
    auto sorted_out_arr = SortChunkedArray(out->ToArrowChunkedArray());
    EXPECT_TRUE(sorted_out_arr->Equals(sorted_in_arr))
        << "expect type = " << in_arr->type()->ToString()
        << ", got type = " << out->ToArrowChunkedArray()->type()->ToString()
        << "\nexpect result = " << sorted_in_arr->ToString()
        << "\nbut actual got result = " << sorted_out_arr->ToString();
  }
}

/// ===========================
/// BucketTest impl
/// ===========================

pb::ExecNode BucketTest::MakeBucketExecNode(const BucketTestCase& tc) {
  test::ExecNodeBuilder builder(Bucket::kOpType);

  builder.SetNodeName("bucket-test");

  // Add join keys
  std::vector<pb::Tensor> key_datas;
  std::vector<pb::Tensor> input_datas;
  std::vector<pb::Tensor> outputs;
  for (const auto& named_tensor : tc.keys) {
    auto data = test::MakePrivateTensorReference(named_tensor.name,
                                                 named_tensor.tensor->Type());
    key_datas.push_back(data);
    input_datas.push_back(data);
    // mock name
    *data.mutable_name() = data.name() + "_out";
    outputs.push_back(data);
  }
  builder.AddInput(Bucket::kJoinKey, key_datas);

  // Add inputs
  for (const auto& named_tensor : tc.datas) {
    auto data = test::MakePrivateTensorReference(named_tensor.name,
                                                 named_tensor.tensor->Type());
    input_datas.push_back(std::move(data));
  }
  builder.AddInput(Bucket::kIn, input_datas);

  // Add outputs
  for (size_t i = 0; i < tc.output_names.size(); ++i) {
    auto out =
        test::MakeTensorAs(tc.output_names[i], input_datas[i + tc.keys.size()]);
    outputs.push_back(std::move(out));
  }
  builder.AddOutput(Bucket::kOut, outputs);

  builder.AddStringsAttr(
      Bucket::kInputPartyCodesAttr,
      std::vector<std::string>{test::kPartyAlice, test::kPartyBob});

  return builder.Build();
}

void BucketTest::FeedInputs(ExecContext* ctx, const BucketTestCase& tc) {
  test::FeedInputsAsPrivate(ctx, tc.datas);
  test::FeedInputsAsPrivate(ctx, tc.keys);
}

}  // namespace scql::engine::op