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
#include "yacl/base/exception.h"

#include "engine/datasource/csvdb_conf.pb.h"

namespace scql::engine {

namespace {

static duckdb::LogicalType ToLogicalType(csv::ColumnType type) {
  switch (type) {
    case csv::ColumnType::LONG:
      return duckdb::LogicalType::BIGINT;
    case csv::ColumnType::DOUBLE:
      return duckdb::LogicalType::DOUBLE;
    case csv::ColumnType::STRING:
      return duckdb::LogicalType::VARCHAR;
    default:
      return duckdb::LogicalType::INVALID;
  }
}

struct CSVTableReplacementScanData : public duckdb::ReplacementScanData {
  const csv::CsvdbConf *csvdb_conf;
};

static std::unique_ptr<duckdb::TableFunctionRef> CSVTableReplacementScan(
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
  std::vector<std::unique_ptr<duckdb::ParsedExpression>> children;
  children.push_back(std::make_unique<duckdb::ConstantExpression>(
      duckdb::Value(csv_tbl->data_path())));
  {
    std::vector<duckdb::Value> types;
    std::vector<duckdb::Value> names;
    for (const auto &col : csv_tbl->columns()) {
      types.emplace_back(ToLogicalType(col.column_type()).ToString());
      names.emplace_back(col.column_name());
    }
    children.push_back(std::make_unique<duckdb::ConstantExpression>(
        duckdb::Value::LIST(duckdb::LogicalType::VARCHAR, std::move(types))));
    children.push_back(std::make_unique<duckdb::ConstantExpression>(
        duckdb::Value::LIST(duckdb::LogicalType::VARCHAR, std::move(names))));
  }
  table_function->function = std::make_unique<duckdb::FunctionExpression>(
      "csv_scan", std::move(children));
  return table_function;
}

static std::unique_ptr<duckdb::FunctionData> CSVScanBind(
    duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
    std::vector<duckdb::LogicalType> &return_types,
    std::vector<std::string> &names) {
  auto return_type_list = duckdb::ListValue::GetChildren(input.inputs[1]);
  for (const auto &type_value : return_type_list) {
    if (type_value.type().id() != duckdb::LogicalTypeId::VARCHAR) {
      throw duckdb::BinderException(
          "csv_scan requires a type specification as string");
    }
    return_types.emplace_back(duckdb::TransformStringToLogicalTypeId(
        duckdb::StringValue::Get(type_value)));
  }

  auto name_list = duckdb::ListValue::GetChildren(input.inputs[2]);
  for (const auto &name_value : name_list) {
    if (name_value.type().id() != duckdb::LogicalTypeId::VARCHAR) {
      throw duckdb::BinderException("csv_scan requires column name as string");
    }
    names.emplace_back(duckdb::StringValue::Get(name_value));
  }

  input.named_parameters["header"] = duckdb::Value::BOOLEAN(true);

  // delegate to ReadCSVBind
  return duckdb::ReadCSVTableFunction::GetFunction().bind(context, input,
                                                          return_types, names);
}

}  // namespace

duckdb::DuckDB DuckDBWrapper::CreateDB(const csv::CsvdbConf *csvdb_conf) {
  auto scan_data = std::make_unique<CSVTableReplacementScanData>();
  scan_data->csvdb_conf = csvdb_conf;

  duckdb::DBConfig config;
  config.replacement_scans.push_back(
      duckdb::ReplacementScan(CSVTableReplacementScan, std::move(scan_data)));

  return duckdb::DuckDB(nullptr, &config);
}

void DuckDBWrapper::CreateCSVScanFunction(duckdb::Connection &conn) {
  auto &context = *conn.context;
  auto &catalog = duckdb::Catalog::GetCatalog(context);

  duckdb::TableFunction read_csv = duckdb::ReadCSVTableFunction::GetFunction();

  duckdb::TableFunction csv_scan(
      "csv_scan",
      {duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
       duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)},
      read_csv.function, CSVScanBind, read_csv.init_global,
      read_csv.init_local);
  csv_scan.table_scan_progress = read_csv.table_scan_progress;
  csv_scan.named_parameters = read_csv.named_parameters;

  duckdb::CreateTableFunctionInfo info(std::move(csv_scan));
  catalog.CreateTableFunction(context, &info);
  return;
}

}  // namespace scql::engine