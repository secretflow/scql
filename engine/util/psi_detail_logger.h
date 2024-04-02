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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "logging.h"
#include "psi/cryptor/ecc_cryptor.h"
#include "psi/ecdh/ecdh_logger.h"
#include "spdlog/spdlog.h"

#include "engine/core/tensor.h"

namespace scql::engine::util {

class EcdhDetailLogger : public psi::ecdh::EcdhLogger {
 public:
  explicit EcdhDetailLogger(std::shared_ptr<spdlog::logger> spd_logger)
      : spd_logger_(std::move(spd_logger)) {}

  void Log(psi::ecdh::EcdhStage stage,
           const std::array<uint8_t, psi::kEccKeySize>& secret_key,
           size_t start_idx, const std::vector<std::string>& input,
           const std::vector<std::string>& output = {}) override;

 private:
  std::shared_ptr<spdlog::logger> spd_logger_;
};

class PsiDetailLogger {
 public:
  explicit PsiDetailLogger(std::shared_ptr<spdlog::logger> spd_logger)
      : spd_logger_(std::move(spd_logger)) {
    ecdh_logger_ = std::make_shared<EcdhDetailLogger>(spd_logger_);
  }

  void LogInput(const std::vector<TensorPtr>& inputs);
  void LogOutput(const TensorPtr& result,
                 const std::vector<TensorPtr>& inputs = {});
  std::shared_ptr<EcdhDetailLogger> GetEcdhLogger() { return ecdh_logger_; }

 private:
  std::shared_ptr<spdlog::logger> spd_logger_;
  std::shared_ptr<EcdhDetailLogger> ecdh_logger_;
};
}  // namespace scql::engine::util
