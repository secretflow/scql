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

#include "engine/util/filepath_helper.h"

#include <filesystem>

#include "absl/strings/match.h"
#include "boost/uuid/uuid.hpp"
#include "boost/uuid/uuid_generators.hpp"
#include "boost/uuid/uuid_io.hpp"
#include "yacl/base/exception.h"
namespace scql::engine::util {

std::string GetS3LikeScheme(const std::string& url) {
  const std::string schemes[] = {kSchemeS3, kSchemeMinIo, kSchemeOss};
  for (auto& scheme : schemes) {
    if (absl::StartsWith(url, scheme)) {
      return scheme;
    }
  }
  return "";
}

void CheckS3LikeUrl(const std::string& path_without_prefix, bool is_restricted,
                    const std::string& restricted_path) {
  // 1. only allow absolute path for s3
  YACL_ENFORCE(GetS3LikeScheme(restricted_path).empty(),
               "restricted_path should remove s3 prefix like 's3://'");
  YACL_ENFORCE(path_without_prefix.find("..") == std::string::npos,
               "path={} cannot contain '..'", path_without_prefix);
  // 2. if restricted, the path_without_prefix must start with restricted_path
  if (is_restricted) {
    YACL_ENFORCE(absl::StartsWith(path_without_prefix, restricted_path),
                 "path={} not start with restricted_path={}",
                 path_without_prefix, restricted_path);
  }
}

std::string CheckAndGetAbsoluteLocalPath(
    const std::string& in_filepath, bool is_restricted,
    const std::string& restricted_filepath) {
  YACL_ENFORCE(!in_filepath.empty());
  auto final_path =
      std::filesystem::weakly_canonical(std::filesystem::path(in_filepath));
  if (is_restricted) {
    YACL_ENFORCE(
        !restricted_filepath.empty(),
        "when path is restricted, the restricted_filepath must not be empty");
    auto restricted_path = std::filesystem::weakly_canonical(
        std::filesystem::path(restricted_filepath));
    YACL_ENFORCE(
        absl::StartsWith(final_path.string(), restricted_path.string()),
        "final_path={} not start with restricted_path={}", final_path.string(),
        restricted_path.string());
  }

  return final_path.string();
}

bool GetAndRemoveS3EndpointPrefix(std::string& endpoint) {
  const std::string http_prefix = "http://";
  const std::string https_prefix = "https://";
  if (absl::StartsWith(endpoint, http_prefix)) {
    endpoint.erase(0, http_prefix.length());
    return false;
  } else if (absl::StartsWith(endpoint, https_prefix)) {
    endpoint.erase(0, https_prefix.length());
  }
  // https by default
  return true;
}

std::filesystem::path CreateDir(const std::filesystem::path& parent_dir,
                                const std::string& dir_name) {
  auto dir = parent_dir / dir_name;
  if (std::filesystem::exists(dir)) {
    YACL_THROW("failed to create dir {}, due to dir already exist",
               dir.string());
  }
  if (!std::filesystem::create_directories(dir)) {
    YACL_THROW("failed to create dir {}", dir.string());
  }
  return dir;
}

std::filesystem::path CreateDirWithRandSuffix(
    const std::filesystem::path& parent_dir, const std::string& dir_name) {
  int tries = 0;

  const int kMaxTries = 10;
  boost::uuids::random_generator uuid_generator;

  do {
    auto uuid_str = boost::uuids::to_string(uuid_generator());
    auto dir = parent_dir / fmt::format("{}-{}", dir_name, uuid_str);
    tries++;
    if (std::filesystem::exists(dir)) {
      continue;
    }
    if (!std::filesystem::create_directory(dir)) {
      YACL_THROW("failed to create dir {}", dir.string());
    }
    return dir;
  } while (tries < kMaxTries);
  YACL_THROW("failed to create dir {}", dir_name);
}

};  // namespace scql::engine::util