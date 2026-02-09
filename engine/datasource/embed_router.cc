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
#include "spdlog/spdlog.h"
#include "yacl/base/exception.h"

#include "engine/datasource/csvdb_conf.pb.h"

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
  auto status =
      google::protobuf::util::JsonStringToMessage(json_str, &router_conf);
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
    auto* ds = router_conf.add_datasources();
    ds->set_id(kDefaultDatasourceId);
    ds->set_name("default db");
    ds->set_kind(DataSourceKind::MYSQL);
    ds->set_connection_str(connection_str);
  }
  {
    auto* rule = router_conf.add_rules();
    rule->set_db("*");
    rule->set_table("*");
    rule->set_datasource_id(kDefaultDatasourceId);
  }

  return FromProto(router_conf);
}

std::unique_ptr<EmbedRouter> EmbedRouter::FromFilePaths(
    const google::protobuf::Map<std::string, std::string>& input_file_paths) {
  EmbedRouterConf router_conf;

  std::string db_name;
  std::string datasource_id;
  csv::CsvdbConf csv_conf;

  for (const auto& [table_ref, file_path] : input_file_paths) {
    // Parse table_ref to extract db and table
    std::vector<absl::string_view> fields = absl::StrSplit(table_ref, '.');
    std::string current_db_name;
    std::string table_name;

    YACL_ENFORCE(fields.size() == 2,
                 "Invalid table_ref format: {}, expected 'db.table' format",
                 table_ref);
    current_db_name = std::string(fields[0]);
    table_name = std::string(fields[1]);

    if (db_name.empty()) {
      db_name = current_db_name;
      datasource_id = fmt::format("csvdb_{}", db_name);

      auto* ds = router_conf.add_datasources();
      ds->set_id(datasource_id);
      ds->set_name(fmt::format("CSV database for {}", db_name));
      ds->set_kind(DataSourceKind::CSVDB);

      csv_conf.set_db_name(db_name);

      SPDLOG_INFO("Creating datasource '{}' for db '{}'", datasource_id,
                  db_name);
    }

    YACL_ENFORCE(db_name == current_db_name,
                 "Multiple databases detected: '{}' and '{}'. Only one "
                 "database is supported in FromFilePaths",
                 db_name, current_db_name);

    auto* csv_tbl = csv_conf.add_tables();
    csv_tbl->set_table_name(table_name);
    csv_tbl->set_data_path(file_path);

    auto* rule = router_conf.add_rules();
    rule->set_db(db_name);
    rule->set_table(table_name);
    rule->set_datasource_id(datasource_id);
  }

  YACL_ENFORCE(!db_name.empty(),
               "No valid table references found in input_file_paths");

  // Serialize CsvdbConf to JSON and set as connection string
  std::string connection_str;
  auto status =
      google::protobuf::util::MessageToJsonString(csv_conf, &connection_str);
  YACL_ENFORCE(status.ok(), "Failed to serialize CsvdbConf to JSON: {}",
               status.ToString());

  bool found_datasource = false;
  for (auto& ds : *router_conf.mutable_datasources()) {
    if (ds.id() == datasource_id) {
      ds.set_connection_str(connection_str);
      found_datasource = true;
      break;
    }
  }

  YACL_ENFORCE(found_datasource, "Failed to find datasource with id: {}",
               datasource_id);

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
