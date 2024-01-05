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

#include "engine/datasource/embed_router.h"

#include "absl/strings/str_split.h"
#include "google/protobuf/util/json_util.h"
#include "yacl/base/exception.h"

namespace scql::engine {

std::vector<DataSource> EmbedRouter::Route(
    const std::vector<std::string>& table_refs) {
  std::vector<std::string> datasource_ids;

  for (const auto& table_ref : table_refs) {
    // 1. find it in table level route rules
    {
      auto iter = table_route_map_.find(table_ref);
      if (iter != table_route_map_.end()) {
        datasource_ids.push_back(iter->second);
        continue;
      }
    }

    // 2. find it in db level route rules
    {
      std::vector<absl::string_view> fields = absl::StrSplit(table_ref, '.');
      if (fields.size() != 2) {
        YACL_THROW("Fail to split '$db.$table' format from: {}", table_ref);
      }

      auto iter = db_route_map_.find(fields[0]);
      if (iter != db_route_map_.end()) {
        datasource_ids.push_back(iter->second);
        continue;
      }
    }

    // 3. default route rule
    if (default_route_datasource_.has_value()) {
      datasource_ids.push_back(default_route_datasource_.value());
      continue;
    }

    YACL_THROW("Fail to find datasource for table={}", table_ref);
  }

  std::vector<DataSource> result(datasource_ids.size());
  for (size_t i = 0; i < datasource_ids.size(); i++) {
    auto iter = datasource_map_.find(datasource_ids[i]);
    if (iter == datasource_map_.end()) {
      YACL_THROW("Fail to find datasource id={}", datasource_ids[i]);
    }
    result[i].CopyFrom(iter->second);
  }
  return result;
}

std::unique_ptr<EmbedRouter> EmbedRouter::FromJsonStr(
    std::string_view json_str) {
  EmbedRouterConf router_conf;
  auto status = google::protobuf::util::JsonStringToMessage(
      google::protobuf::StringPiece(json_str), &router_conf);
  if (!status.ok()) {
    YACL_THROW("Fail to parse json_str to `EmbedRouterConf`, reason={}",
               status.ToString());
  }

  return FromProto(router_conf);
}

std::unique_ptr<EmbedRouter> EmbedRouter::FromConnectionStr(
    const std::string& connection_str) {
  EmbedRouterConf router_conf;
  const std::string kDefaultDatasourceId = "ds001";
  {
    auto ds = router_conf.add_datasources();
    ds->set_id(kDefaultDatasourceId);
    ds->set_name("default db");
    ds->set_kind(DataSourceKind::MYSQL);
    ds->set_connection_str(connection_str);
  }
  {
    auto rule = router_conf.add_rules();
    rule->set_db("*");
    rule->set_table("*");
    rule->set_datasource_id(kDefaultDatasourceId);
  }

  return FromProto(router_conf);
}

std::unique_ptr<EmbedRouter> EmbedRouter::FromProto(
    const EmbedRouterConf& router_conf) {
  auto router = std::make_unique<EmbedRouter>();
  // initialize router with router_conf
  {
    for (const auto& ds : router_conf.datasources()) {
      auto ret = router->datasource_map_.insert({ds.id(), ds});
      if (!ret.second) {
        YACL_THROW("Fail to insert datasource: duplicate id={}", ds.id());
      }
    }

    for (const auto& rule : router_conf.rules()) {
      // check rule target <datasource_id> exists
      YACL_ENFORCE(router->datasource_map_.contains(rule.datasource_id()),
                   "Fail to find associated datasource for rule={}",
                   rule.ShortDebugString());

      if (rule.db() == "*" && rule.table() == "*") {
        if (router->default_route_datasource_.has_value()) {
          YACL_THROW("Fail to set default route rule: exists already.");
        }
        router->default_route_datasource_ = rule.datasource_id();
        continue;
      } else if (rule.table() == "*") {  // rule.db() != "*"
        router->db_route_map_[rule.db()] = rule.datasource_id();
        continue;
      } else if (rule.db() == "*") {  // rule.table() != "*"
        YACL_ENFORCE(
            "Fail to parse rule: not support format (db=\"*\",table!=\"*\"), "
            "rule={}",
            rule.ShortDebugString());
      } else {  // rule.db() != "*" && rule.table() != "*"
        router
            ->table_route_map_[fmt::format("{}.{}", rule.db(), rule.table())] =
            rule.datasource_id();
        continue;
      }
    }
  }

  return router;
}

}  // namespace scql::engine