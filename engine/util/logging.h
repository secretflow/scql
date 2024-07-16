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

#include <memory>
#include <string>

#include "spdlog/spdlog.h"

namespace scql::engine::util {

struct LogOptions {
  bool enable_console_logger = false;
  bool enable_psi_detail_logger = false;
  bool enable_session_logger_separation = false;
  std::string log_dir = "logs";
  size_t max_log_file_size = 500 * 1024 * 1024;
  size_t max_log_file_count = 10;
  std::string log_level = "info";
};

// Setup default logger "scqlengine", and use it to sink brpc log.
void SetupLogger(const LogOptions& opts = LogOptions());

std::shared_ptr<spdlog::logger> CreateDetailLogger(
    const std::string& logger_name, const std::string& logger_file_name,
    const LogOptions& opts = LogOptions());

std::shared_ptr<spdlog::logger> DupDefaultLogger(
    const std::string& logger_name);

std::shared_ptr<spdlog::logger> CreateLogger(
    const std::string& logger_name, const std::string& logger_file_name,
    const LogOptions& opts);
}  // namespace scql::engine::util