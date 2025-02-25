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

#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <random>
#include <vector>

#include "arrow/builder.h"
#include "benchmark/benchmark.h"
#include "yacl/crypto/rand/rand.h"

#include "engine/util/psi/cipher_intersection.h"

static void BM_InCalResult(benchmark::State& state) {
  for (auto _ : state) {
    state.PauseTiming();
    size_t len = state.range(0);
    std::vector<size_t> index_v(len);
    for (size_t i = 0; i < len; ++i) {
      index_v[i] = i;
    }
    std::mt19937 rng(yacl::crypto::SecureRandU64());
    std::shuffle(index_v.begin(), index_v.end(), rng);
    scql::engine::util::InResultResolver resolver(len);
    state.ResumeTiming();
    for (const auto& idx : index_v) {
      resolver.Set(idx, true);
    }
    resolver.Finalize();
  }
}

BENCHMARK(BM_InCalResult)->Unit(benchmark::kMillisecond)->Arg(1000 * 10000);
