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

#include "engine/datasource/duckdb_wrapper.h"

#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "gflags/gflags.h"
#include "yacl/base/exception.h"

#include "engine/util/filepath_helper.h"

#include "engine/datasource/csvdb_conf.pb.h"

namespace scql::engine {

DEFINE_bool(enable_restricted_read_path, true,
            "whether restrict path for file to read");
DEFINE_string(
    restricted_read_path, "./data",
    "in where the file is allowed to read if enable restricted read path");
DEFINE_string(csv_null_str, "NULL",
              "specifies the string that represents a NULL value.");

namespace {

static duckdb::LogicalType ToLogicalType(csv::ColumnType type) {
  switch (type) {
    case csv::ColumnType::LONG:
      return duckdb::LogicalType::BIGINT;
    case csv::ColumnType::FLOAT:
      return duckdb::LogicalType::FLOAT;
    case csv::ColumnType::DOUBLE:
      return duckdb::LogicalType::DOUBLE;
    case csv::ColumnType::STRING:
      return duckdb::LogicalType::VARCHAR;
    // in duckdb, date type represent year, month and day:
    // http://duckdb.org/docs/sql/data_types/date
    case csv::ColumnType::DATETIME:
      return duckdb::LogicalType::DATE;
    case csv::ColumnType::TIMESTAMP:
      return duckdb::LogicalType::TIMESTAMP_S;
    default:
      return duckdb::LogicalType::INVALID;
  }
}

struct CSVTableReplacementScanData : public duckdb::ReplacementScanData {
  const csv::CsvdbConf *csvdb_conf;
};

static duckdb::unique_ptr<duckdb::TableRef> CSVTableReplacementScan(
    duckdb::ClientContext &context, const std::string &table_name,
    duckdb::ReplacementScanData *data) {
  auto scan_data = dynamic_cast<CSVTableReplacementScanData *>(data);
  if (!scan_data || !scan_data->csvdb_conf) {
    return nullptr;
  }

  const csv::CsvTableConf *csv_tbl = nullptr;
  for (int i = 0; i < scan_data->csvdb_conf->tables_size(); ++i) {
    const std::string full_table_name =
        fmt::format("{}.{}", scan_data->csvdb_conf->db_name(),
                    scan_data->csvdb_conf->tables(i).table_name());
    if (full_table_name == table_name) {
      csv_tbl = &scan_data->csvdb_conf->tables(i);
      break;
    }
  }
  if (!csv_tbl) {
    return nullptr;
  }

  auto table_function = std::make_unique<duckdb::TableFunctionRef>();
  std::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> children;
  children.emplace_back(duckdb::make_uniq<duckdb::ConstantExpression>(
      duckdb::Value(util::GetAbsolutePath(csv_tbl->data_path(),
                                          FLAGS_enable_restricted_read_path,
                                          FLAGS_restricted_read_path))));
  {
    std::vector<duckdb::Value> names;
    std::vector<duckdb::Value> types;
    for (const auto &col : csv_tbl->columns()) {
      names.emplace_back(col.column_name());
      types.emplace_back(ToLogicalType(col.column_type()).ToString());
    }
    children.emplace_back(duckdb::make_uniq<duckdb::ConstantExpression>(
        duckdb::Value::LIST(duckdb::LogicalType::VARCHAR, std::move(names))));
    children.emplace_back(duckdb::make_uniq<duckdb::ConstantExpression>(
        duckdb::Value::LIST(duckdb::LogicalType::VARCHAR, std::move(types))));
  }

  table_function->function = duckdb::make_uniq<duckdb::FunctionExpression>(
      "csv_scan", std::move(children));
  return table_function;
}

static duckdb::unique_ptr<duckdb::FunctionData> CSVScanBind(
    duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
    duckdb::vector<duckdb::LogicalType> &return_types,
    duckdb::vector<std::string> &names) {
  auto name_list = duckdb::ListValue::GetChildren(input.inputs[1]);
  auto type_list = duckdb::ListValue::GetChildren(input.inputs[2]);
  if (name_list.size() != type_list.size()) {
    throw duckdb::BinderException(
        "csv_scan: name_list & type_list size mismatched");
  }
  duckdb::child_list_t<duckdb::Value> columns;
  for (size_t i = 0; i < name_list.size(); i++) {
    if (name_list[i].type().id() != duckdb::LogicalTypeId::VARCHAR ||
        type_list[i].type().id() != duckdb::LogicalTypeId::VARCHAR) {
      throw duckdb::BinderException(
          "csv_scan requires a column name & type specification as string");
    }
    columns.emplace_back(
        std::make_pair(duckdb::StringValue::Get(name_list[i]),
                       duckdb::StringValue::Get(type_list[i])));
  }
  input.named_parameters["nullstr"] = duckdb::Value(FLAGS_csv_null_str);
  input.named_parameters["header"] = duckdb::Value::BOOLEAN(true);
  input.named_parameters["columns"] = duckdb::Value::STRUCT(std::move(columns));

  // delegate to ReadCSVBind
  return duckdb::ReadCSVTableFunction::GetFunction().bind(context, input,
                                                          return_types, names);
}

void CheckTablePaths(const csv::CsvdbConf &csvdb_conf) {
  for (const auto &table_conf : csvdb_conf.tables()) {
    // If path not illegal, exception will be thrown.
    util::GetAbsolutePath(table_conf.data_path(),
                          FLAGS_enable_restricted_read_path,
                          FLAGS_restricted_read_path);
  }
}

}  // namespace

duckdb::DuckDB DuckDBWrapper::CreateDB(const csv::CsvdbConf *csvdb_conf) {
  CheckTablePaths(*csvdb_conf);
  auto scan_data = duckdb::make_uniq<CSVTableReplacementScanData>();
  scan_data->csvdb_conf = csvdb_conf;

  duckdb::DBConfig config;
  config.replacement_scans.push_back(
      duckdb::ReplacementScan(CSVTableReplacementScan, std::move(scan_data)));

  return duckdb::DuckDB(nullptr, &config);
}

void DuckDBWrapper::CreateCSVScanFunction(duckdb::Connection &conn) {
  auto &context = *conn.context;
  auto &catalog = duckdb::Catalog::GetSystemCatalog(context);

  duckdb::TableFunction read_csv = duckdb::ReadCSVTableFunction::GetFunction();

  duckdb::TableFunction csv_scan(
      "csv_scan",
      {duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
       duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)},
      read_csv.function, CSVScanBind, read_csv.init_global,
      read_csv.init_local);
  csv_scan.table_scan_progress = read_csv.table_scan_progress;
  csv_scan.pushdown_complex_filter = read_csv.pushdown_complex_filter;
  csv_scan.serialize = read_csv.serialize;
  csv_scan.deserialize = read_csv.deserialize;
  csv_scan.get_batch_index = read_csv.get_batch_index;
  csv_scan.cardinality = read_csv.cardinality;
  csv_scan.projection_pushdown = read_csv.projection_pushdown;

  csv_scan.named_parameters = read_csv.named_parameters;

  duckdb::CreateTableFunctionInfo info(std::move(csv_scan));
  catalog.CreateTableFunction(context, &info);
  return;
}

}  // namespace scql::engine