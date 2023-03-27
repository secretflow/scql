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

#include "engine/util/logging.h"

#include "butil/file_util.h"
#include "fmt/format.h"
#include "fmt/printf.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"
#include "yacl/base/exception.h"

namespace scql::engine::util {

// clang-format off
/// custom formatting:
/// https://github.com/gabime/spdlog/wiki/3.-Custom-formatting
/// example: ```2019-11-29 06:58:54.633 [info] [log_test.cpp:TestBody:16] The answer is 42.```
// clang-format on
static const char* kFormatPattern = "%Y-%m-%d %H:%M:%S.%e [%l] [%s:%!:%#] %v";

namespace {

static const char* kBrpcUnknownFuncname = "BRPC";

spdlog::level::level_enum FromBrpcLogSeverity(int severity) {
  spdlog::level::level_enum level = spdlog::level::off;
  if (severity == ::logging::BLOG_INFO) {
    level = spdlog::level::debug;
  } else if (severity == ::logging::BLOG_NOTICE) {
    level = spdlog::level::info;
  } else if (severity == ::logging::BLOG_WARNING) {
    level = spdlog::level::warn;
  } else if (severity == ::logging::BLOG_ERROR) {
    level = spdlog::level::err;
  } else if (severity == ::logging::BLOG_FATAL) {
    level = spdlog::level::critical;
  } else {
    level = spdlog::level::warn;
  }
  return level;
}

class EnginelogSink : public ::logging::LogSink {
 public:
  bool OnLogMessage(int severity, const char* file, int line,
                    const butil::StringPiece& log_content) override {
    spdlog::level::level_enum log_level = FromBrpcLogSeverity(severity);
    spdlog::log(spdlog::source_loc{file, line, kBrpcUnknownFuncname}, log_level,
                "{}", fmt::string_view(log_content.data(), log_content.size()));
    return true;
  }
};

void SinkBrpcLogWithDefaultLogger() {
  static EnginelogSink nlog_sink;
  ::logging::SetLogSink(&nlog_sink);
  ::logging::SetMinLogLevel(::logging::BLOG_NOTICE);
}

}  // namespace

void SetupLogger(LogOptions opts) {
  // 1. mkdir logs/ if not exists
  const butil::FilePath logs_dir{opts.log_dir};
  {
    butil::File::Error error;
    YACL_ENFORCE(butil::CreateDirectoryAndGetError(logs_dir, &error),
                 "Failed to create directory={}: {}", logs_dir.value(),
                 butil::File::ErrorToString(error));
  }

  // 2. setup scqlengine logger, make it default
  auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
      logs_dir.Append("scqlengine.log").value(), opts.max_log_file_size,
      opts.max_log_file_count);

  std::vector<spdlog::sink_ptr> sinks{file_sink};
  if (opts.enable_console_logger) {
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    sinks.push_back(std::move(console_sink));
  }

  auto root_logger = std::make_shared<spdlog::logger>(
      "scqlengine", sinks.begin(), sinks.end());
  root_logger->set_level(spdlog::level::info);
  root_logger->set_pattern(kFormatPattern);
  root_logger->flush_on(spdlog::level::info);
  spdlog::set_default_logger(root_logger);

  // 3. sink brpc log.
  SinkBrpcLogWithDefaultLogger();
}

}  // namespace scql::engine::util