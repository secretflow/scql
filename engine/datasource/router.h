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

#include "engine/datasource/datasource.pb.h"

namespace scql::engine {

/// @brief Datasource router
class Router {
 public:
  virtual ~Router() = default;

  /// @return DataSources which the table_refs belong to.
  virtual std::vector<DataSource> Route(
      const std::vector<std::string>& table_refs) = 0;
};

}  // namespace scql::engine