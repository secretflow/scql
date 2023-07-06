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
#include "yacl/base/exception.h"

namespace scql::engine::util {

std::string GetAbsolutePath(const std::string& in_filepath, bool is_restricted,
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

};  // namespace scql::engine::util