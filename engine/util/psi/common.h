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

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "spdlog/logger.h"

#include "engine/framework/exec.h"

namespace scql::engine::util {

static constexpr int64_t kInnerJoin = 0;
static constexpr int64_t kLeftJoin = 1;
static constexpr int64_t kRightJoin = 2;
static constexpr size_t kNumBins = 64;

enum class PsiAlgo : int64_t {
  kAutoPsi = 0,
  kEcdhPsi = 1,
  kOprfPsi = 2,
  kRr22Psi = 3,
  kAlgoNums,  // Sentinel Value
};

struct PsiSizeInfo {
  size_t self_size = 0;
  size_t peer_size = 0;
};

struct PsiPlan {
  bool unbalanced = false;
  bool is_server = false;
  PsiSizeInfo psi_size_info;
};

struct PsiExecutionInfoTable {
  decltype(std::chrono::system_clock::now()) start_time;
  size_t self_size;
  size_t peer_size;
  int64_t result_size;
};

PsiPlan GetPsiPlan(int64_t self_length, int64_t peer_length,
                   int64_t unbalance_psi_ratio_threshold,
                   int64_t unbalance_psi_larger_party_rows_count_threshold);

PsiPlan GetOprfPsiPlan(int64_t self_length, int64_t peer_length);

PsiPlan CoordinatePsiPlan(ExecContext* ctx, bool force_unbalanced);

class BatchFinishedCb {
 public:
  BatchFinishedCb(std::shared_ptr<spdlog::logger> logger, std::string task_id,
                  size_t batch_total);

  void operator()(size_t batch_count);

 private:
  const std::string task_id_;
  size_t batch_total_;
  std::shared_ptr<spdlog::logger> logger_;
};

size_t ExchangeSetSize(const std::shared_ptr<yacl::link::Context>& link_ctx,
                       size_t items_size);

/// @brief restore back the `in` output order via sorting output by index
}  // namespace scql::engine::util
