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

#include <filesystem>
#include <optional>
#include <string>

#include "absl/strings/match.h"
#include "spdlog/spdlog.h"

namespace scql::engine::util {

static constexpr char kSchemeS3[] = "s3://";
static constexpr char kSchemeMinIo[] = "minio://";
static constexpr char kSchemeOss[] = "oss://";

std::string GetS3LikeScheme(const std::string& url);

// @param[path_without_prefix] should be the file path without prefix, e.g
// "bucket/path/to/file.txt"
// @param[restricted_path] should be a path without prefix, e.g: "bucket"
void CheckS3LikeUrl(const std::string& path_without_prefix, bool is_restricted,
                    const std::string& restricted_path);

std::string CheckAndGetAbsoluteLocalPath(
    const std::string& in_filepath, bool is_restricted,
    const std::string& restricted_filepath);

// Returns true if the endpoint prefix is https, false if it is http, and true
// by default.
bool GetAndRemoveS3EndpointPrefix(std::string& endpoint);

class ScopedDir {
 public:
  ScopedDir(const std::filesystem::path& dir) : dir_(dir) {}

  const std::filesystem::path& path() const { return dir_; }

  ~ScopedDir() {
    if (!dir_.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(dir_, ec);
      if (ec.value() != 0) {
        SPDLOG_WARN("can not remove tmp dir: {}, msg: {}", dir_.string(),
                    ec.message());
      }
    }
  }

 private:
  std::filesystem::path dir_;
};

std::filesystem::path CreateDir(const std::filesystem::path& parent_dir,
                                const std::string& dir_name);

std::filesystem::path CreateDirWithRandSuffix(
    const std::filesystem::path& parent_dir, const std::string& dir_name);

}  // namespace scql::engine::util