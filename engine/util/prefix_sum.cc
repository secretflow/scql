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

#include "engine/util/prefix_sum.h"

#include "absl/numeric/bits.h"
#include "libspu/kernel/hlo/indexing.h"

namespace scql::engine::util {

namespace {

using IndexTuple = std::pair<spu::Index, spu::Index>;
using ScanFnParallel = std::function<void(IndexTuple&)>;

// Implements the most naive version of prefix sum tree structure.
// Refer to
// "Circuit representation of a work-efficient 16-input parallel prefix sum"
// https://en.wikipedia.org/wiki/Prefix_sum#/media/File:Prefix_sum_16.svg
void ScanParallel(std::size_t n, const ScanFnParallel& parallel_fn) {
  size_t k = absl::bit_width(n - 1U);

  if (n < 2) {
    return;
  }

  for (size_t i = 1; i < (k + 1); i++) {
    IndexTuple it;
    for (size_t j = ((1 << i) - 1); j < n; j += (1 << i)) {
      it.first.emplace_back(j);
      it.second.emplace_back(j - (1 << (i - 1)));
    }
    if (!it.first.empty()) {
      parallel_fn(it);
    }
  }

  for (size_t i = (k - 1); i > 0; i--) {
    IndexTuple it;
    for (size_t j = (3 * (1 << (i - 1)) - 1); j < n; j += (1 << i)) {
      it.first.emplace_back(j);
      it.second.emplace_back(j - (1 << (i - 1)));
    }

    if (!it.first.empty()) {
      parallel_fn(it);
    }
  }
}

}  // namespace

// Reference: "Scape: Scalable Collaborative Analytics System on Private
// Database with Malicious Security", Fig. 13
//
// @param[origin_group_mask] 0 means start of the group while 1 means other
// conditions
spu::Value Scan(spu::SPUContext* ctx, const spu::Value& origin_value,
                const spu::Value& origin_group_mask, const ScanFn& scan_fn) {
  spu::Value value = origin_value.clone();
  spu::Value group_mask = origin_group_mask.clone();
  auto parallel_scan_fn = [&](IndexTuple& index_tuple) {
    auto lhs_v = spu::kernel::hlo::LinearGather(ctx, value, index_tuple.first);
    auto lhs_gm =
        spu::kernel::hlo::LinearGather(ctx, group_mask, index_tuple.first);

    auto rhs_v = spu::kernel::hlo::LinearGather(ctx, value, index_tuple.second);
    auto rhs_gm =
        spu::kernel::hlo::LinearGather(ctx, group_mask, index_tuple.second);

    const auto [new_v, new_gm] = scan_fn(lhs_v, lhs_gm, rhs_v, rhs_gm);

    YACL_ENFORCE_EQ(value.dtype(), new_v.dtype());
    YACL_ENFORCE_EQ(group_mask.dtype(), new_gm.dtype());

    spu::kernel::hlo::LinearScatterInPlace(ctx, value, new_v,
                                           index_tuple.first);
    spu::kernel::hlo::LinearScatterInPlace(ctx, group_mask, new_gm,
                                           index_tuple.first);
  };
  ScanParallel(origin_value.shape()[0], parallel_scan_fn);
  return value;
}

};  // namespace scql::engine::util