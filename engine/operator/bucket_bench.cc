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

#include <cstddef>
#include <filesystem>
#include <memory>
#include <utility>

#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "benchmark/benchmark.h"

#include "engine/core/tensor.h"
#include "engine/core/tensor_constructor.h"
#include "engine/operator/bucket.h"
#include "engine/operator/test_util.h"
#include "engine/util/filepath_helper.h"

namespace scql::engine::op {

struct BucketTestCase {
  std::vector<test::NamedTensor> keys;
  std::vector<test::NamedTensor> datas;
  std::vector<std::string> output_names;
};

test::NamedTensor CreateNamedTensor(const std::string& name, TensorPtr t) {
  return test::NamedTensor(name, std::move(t));
};

pb::ExecNode MakeBucketExecNode(const BucketTestCase& tc) {
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
    auto out = test::MakeTensorAs(tc.output_names[i], input_datas[i]);
    outputs.push_back(std::move(out));
  }
  builder.AddOutput(Bucket::kOut, outputs);
  builder.AddStringsAttr(
      Bucket::kInputPartyCodesAttr,
      std::vector<std::string>{test::kPartyAlice, test::kPartyBob});

  return builder.Build();
}

void FeedInputs(ExecContext* ctx, const BucketTestCase& tc) {
  test::FeedInputsAsPrivate(ctx, tc.datas);
  test::FeedInputsAsPrivate(ctx, tc.keys);
}

void RunBucketBench(std::vector<TensorPtr>& join_keys,
                    std::vector<TensorPtr>& payloads) {
  // Given
  BucketTestCase tc;
  for (size_t i = 0; i < join_keys.size(); ++i) {
    std::string t_name = absl::StrCat("key_", i);
    tc.keys.push_back(CreateNamedTensor(t_name, join_keys[i]));
    tc.output_names.push_back(absl::StrCat(t_name, "_out"));
  }
  for (size_t i = 0; i < payloads.size(); ++i) {
    std::string t_name = absl::StrCat("payload_", i);
    tc.datas.push_back(CreateNamedTensor(t_name, join_keys[i]));
    tc.output_names.push_back(absl::StrCat(t_name, "_out"));
  }
  auto node = MakeBucketExecNode(tc);
  auto session = test::Make1PCSession();
  auto options = session->GetStreamingOptions();
  // 1kw
  options.batch_row_num = 20 * 1000 * 1000;
  options.streaming_row_num_threshold = 20 * 1000 * 1000;
  session->SetStreamingOptions(options);
  ExecContext ctx(node, session.get());
  // feed inputs, test copy from alice to bob.
  FeedInputs(&ctx, tc);
  Bucket op;
  // When
  op.Run(&ctx);
}

TensorPtr CreateTmpTensor(const std::filesystem::path& path,
                          const std::shared_ptr<arrow::Field>& field,
                          size_t num) {
  constexpr arrow::random::SeedType randomSeed = 0x0ff1ce;
  constexpr size_t batch_size = 1000 * 1000;
  arrow::ArrayVector arrays;
  for (size_t i = 0; i < num; i += batch_size) {
    arrays.push_back(
        arrow::random::GenerateArray(*field, batch_size, randomSeed));
  }
  arrow::FieldVector fields = {field};
  auto schema = std::make_shared<arrow::Schema>(fields);
  auto expected_chunked_array = std::make_shared<arrow::ChunkedArray>(arrays);
  scql::engine::TensorWriter writer(schema, path.string());
  writer.WriteBatch(*expected_chunked_array);
  scql::engine::TensorPtr t;
  writer.Finish(&t);
  return t;
}

}  // namespace scql::engine::op

static void BM_BucketTest(benchmark::State& state) {
  for (auto _ : state) {
    state.PauseTiming();
    size_t array_num_rows = state.range(0);
    auto path = scql::engine::util::CreateDirWithRandSuffix(
        std::filesystem::temp_directory_path(), "test");
    std::vector<scql::engine::TensorPtr> join_keys;
    auto field = std::make_shared<arrow::Field>("a", arrow::large_utf8());
    join_keys.push_back(scql::engine::op::CreateTmpTensor(
        scql::engine::util::CreateDirWithRandSuffix(path, "keys"), field,
        array_num_rows));
    field = std::make_shared<arrow::Field>("b", arrow::large_utf8());
    std::vector<scql::engine::TensorPtr> payloads;
    payloads.push_back(scql::engine::op::CreateTmpTensor(
        scql::engine::util::CreateDirWithRandSuffix(path, "payload"), field,
        array_num_rows));
    state.ResumeTiming();
    scql::engine::op::RunBucketBench(join_keys, payloads);
    state.PauseTiming();
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
    state.ResumeTiming();
  }
}

BENCHMARK(BM_BucketTest)->Unit(benchmark::kMillisecond)->Arg(500 * 1000 * 1000);
