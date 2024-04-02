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
#include <optional>
#include <string_view>

#include "absl/container/flat_hash_map.h"

#include "engine/datasource/router.h"

#include "engine/datasource/embed_router.pb.h"

namespace scql::engine {

// EmbedRouter is built from static configuration file
class EmbedRouter final : public Router {
 public:
  virtual ~EmbedRouter() {}

  std::vector<DataSource> Route(
      const std::vector<std::string>& table_refs) override;

 public:
  static std::unique_ptr<EmbedRouter> FromJsonStr(std::string_view json_str);

  // Only support MYSQL's connection string temporarily.
  // TODO(jingshi) : Add DataSourceKind: HTTP/CVSDB... if needed.
  static std::unique_ptr<EmbedRouter> FromConnectionStr(
      const std::string& connection_str);

 private:
  static std::unique_ptr<EmbedRouter> FromProto(
      const EmbedRouterConf& router_conf);

 private:
  // $datasource_id --> datasource spec
  absl::flat_hash_map<std::string, DataSource> datasource_map_;

  // table level route rule
  // ($db,$table) -> datasource_id
  absl::flat_hash_map<std::string, std::string> table_route_map_;

  // database level route rule
  // ($db) -> datasource_id
  absl::flat_hash_map<std::string, std::string> db_route_map_;

  // default route rule
  std::optional<std::string> default_route_datasource_;
};

}  // namespace scql::engine