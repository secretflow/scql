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

#include <filesystem>

#include "arrow/array.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "benchmark/benchmark.h"
#include "yacl/utils/parallel.h"

#include "engine/util/disk/arrow_reader.h"
#include "engine/util/disk/arrow_writer.h"
#include "engine/util/filepath_helper.h"

static void BM_WriteTest(benchmark::State& state) {
  for (auto _ : state) {
    state.PauseTiming();
    constexpr arrow::random::SeedType randomSeed = 0x0ff1ce;
    arrow::ArrayVector arrays;
    constexpr size_t array_num = 3;
    size_t array_num_rows = state.range(0);
    auto field = std::make_shared<arrow::Field>("a", arrow::int64());
    for (size_t i = 0; i < array_num; ++i) {
      arrays.push_back(
          arrow::random::GenerateArray(*field, array_num_rows, randomSeed));
    }
    arrow::FieldVector fields = {field};
    auto schema = std::make_shared<arrow::Schema>(fields);
    auto expected_chunked_array = std::make_shared<arrow::ChunkedArray>(arrays);
    auto path = scql::engine::util::CreateDirWithRandSuffix(
                    std::filesystem::temp_directory_path(), "test") /
                "test.arrow";

    state.ResumeTiming();
    scql::engine::util::disk::ArrowWriter writer(schema, path.string());
    writer.WriteBatch(*expected_chunked_array);
    state.PauseTiming();
    std::error_code ec;
    std::filesystem::remove(path, ec);
    state.ResumeTiming();
  }
}

static void BM_ParallelWriteTest(benchmark::State& state) {
  for (auto _ : state) {
    state.PauseTiming();
    constexpr arrow::random::SeedType randomSeed = 0x0ff1ce;
    arrow::ArrayVector arrays;
    constexpr size_t array_num = 3;
    size_t array_num_rows = state.range(0);
    auto field = std::make_shared<arrow::Field>("a", arrow::int64());
    for (size_t i = 0; i < array_num; ++i) {
      arrays.push_back(
          arrow::random::GenerateArray(*field, array_num_rows, randomSeed));
    }
    arrow::FieldVector fields = {field};
    auto schema = std::make_shared<arrow::Schema>(fields);
    auto expected_chunked_array = std::make_shared<arrow::ChunkedArray>(arrays);
    auto path = scql::engine::util::CreateDirWithRandSuffix(
        std::filesystem::temp_directory_path(), "test");

    state.ResumeTiming();
    yacl::parallel_for(0, state.range(1), [&](int64_t begin, int64_t end) {
      for (int64_t i = begin; i < end; ++i) {
        auto sub_path = scql::engine::util::CreateDir(path, std::to_string(i));
        scql::engine::util::disk::ArrowWriter writer(
            schema, (sub_path / "test.arrow").string());
        writer.WriteBatch(*expected_chunked_array);
      }
    });
    state.PauseTiming();
    std::error_code ec;
    std::filesystem::remove(path, ec);
    state.ResumeTiming();
  }
}

static void BM_WriteSmallFileTest(benchmark::State& state) {
  for (auto _ : state) {
    state.PauseTiming();
    constexpr arrow::random::SeedType randomSeed = 0x0ff1ce;
    arrow::ArrayVector arrays;
    size_t array_num = 300 * 1000 * 1000 / state.range(0);
    size_t array_num_rows = state.range(0);
    auto field = std::make_shared<arrow::Field>("a", arrow::int64());
    for (size_t i = 0; i < array_num; ++i) {
      arrays.push_back(
          arrow::random::GenerateArray(*field, array_num_rows, randomSeed));
    }
    arrow::FieldVector fields = {field};
    auto schema = std::make_shared<arrow::Schema>(fields);
    auto expected_chunked_array = std::make_shared<arrow::ChunkedArray>(arrays);
    auto path = scql::engine::util::CreateDirWithRandSuffix(
                    std::filesystem::temp_directory_path(), "test") /
                "test.arrow";

    state.ResumeTiming();
    scql::engine::util::disk::ArrowWriter writer(schema, path.string());
    writer.WriteBatch(*expected_chunked_array);
    state.PauseTiming();
    std::error_code ec;
    std::filesystem::remove(path, ec);
    state.ResumeTiming();
  }
}

static void BM_ReadTest(benchmark::State& state) {
  for (auto _ : state) {
    state.PauseTiming();
    constexpr arrow::random::SeedType randomSeed = 0x0ff1ce;
    arrow::ArrayVector arrays;
    size_t array_num = 300 * 1000 * 1000 / state.range(0);
    size_t array_num_rows = state.range(0);
    auto field = std::make_shared<arrow::Field>("a", arrow::int64());
    for (size_t i = 0; i < array_num; ++i) {
      arrays.push_back(
          arrow::random::GenerateArray(*field, array_num_rows, randomSeed));
    }
    arrow::FieldVector fields = {field};
    auto schema = std::make_shared<arrow::Schema>(fields);
    auto path = std::filesystem::temp_directory_path() / "test.arrow";
    auto expected_chunked_array = std::make_shared<arrow::ChunkedArray>(arrays);
    {
      scql::engine::util::disk::ArrowWriter writer(schema, path.string());
      writer.WriteBatch(*expected_chunked_array);
    }
    state.ResumeTiming();
    scql::engine::util::disk::FileBatchReader reader(path.string());
    while (reader.ReadNext(state.range(0)));
    state.PauseTiming();
    std::error_code ec;
    std::filesystem::remove(path, ec);
    state.ResumeTiming();
  }
}

static void BM_ParallelWriteSmallFileTest(benchmark::State& state) {
  for (auto _ : state) {
    state.PauseTiming();
    constexpr arrow::random::SeedType randomSeed = 0x0ff1ce;
    arrow::ArrayVector arrays;
    size_t array_num = 300 * 1000 * 1000 / state.range(0) / 20;
    size_t array_num_rows = state.range(0);
    auto field = std::make_shared<arrow::Field>("a", arrow::int64());
    for (size_t i = 0; i < array_num; ++i) {
      arrays.push_back(
          arrow::random::GenerateArray(*field, array_num_rows, randomSeed));
    }
    arrow::FieldVector fields = {field};
    auto schema = std::make_shared<arrow::Schema>(fields);
    auto expected_chunked_array = std::make_shared<arrow::ChunkedArray>(arrays);
    auto path = scql::engine::util::CreateDirWithRandSuffix(
        std::filesystem::temp_directory_path(), "test");
    yacl::parallel_for(0, 20, [&](int64_t begin, int64_t end) {
      for (int64_t i = begin; i < end; ++i) {
        auto sub_path = scql::engine::util::CreateDir(path, std::to_string(i));
        scql::engine::util::disk::ArrowWriter writer(
            schema, (sub_path / "test.arrow").string());
        writer.WriteBatch(*expected_chunked_array);
      }
    });
    state.PauseTiming();
    std::error_code ec;
    std::filesystem::remove(path, ec);
    state.ResumeTiming();
  }
}

BENCHMARK(BM_WriteTest)
    ->Unit(benchmark::kMillisecond)
    ->Arg(10000000)
    ->Arg(100000000);

BENCHMARK(BM_ParallelWriteTest)
    ->Unit(benchmark::kMillisecond)
    ->RangeMultiplier(2)
    ->Args({2000000, 5})
    ->Args({20000000, 5})
    ->Args({2000000, 10})
    ->Args({20000000, 10})
    ->Args({2000000, 20})
    ->Args({20000000, 20});

BENCHMARK(BM_WriteSmallFileTest)
    ->Unit(benchmark::kMillisecond)
    ->Arg(5000)
    ->Arg(50000)
    ->Arg(500000);

BENCHMARK(BM_ReadTest)
    ->Unit(benchmark::kMillisecond)
    ->Arg(5000)
    ->Arg(50000)
    ->Arg(500000);

BENCHMARK(BM_ParallelWriteSmallFileTest)
    ->Unit(benchmark::kMillisecond)
    ->Arg(5000)
    ->Arg(50000)
    ->Arg(500000);
