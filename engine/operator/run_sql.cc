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

#include "engine/operator/run_sql.h"

#include "spdlog/spdlog.h"
#include "yacl/base/exception.h"

#include "engine/audit/audit_log.h"

namespace scql::engine::op {

const std::string RunSQL::kOpType("RunSQL");

const std::string& RunSQL::Type() const { return kOpType; }

void RunSQL::Execute(ExecContext* ctx) {
  const auto start_time = std::chrono::system_clock::now();
  auto logger = ctx->GetActiveLogger();
  std::string select = ctx->GetStringValueFromAttribute(kSQLAttr);

  std::vector<std::string> table_refs =
      ctx->GetStringValuesFromAttribute(kTableRefsAttr);
  YACL_ENFORCE(table_refs.size() > 0, "attr: {} should not be empty",
               kTableRefsAttr);

  std::vector<DataSource> datasource_specs =
      ctx->GetDatasourceRouter()->Route(table_refs);

  YACL_ENFORCE(datasource_specs.size() == table_refs.size(),
               "the size of route result mismatch with table_refs size");

  if (datasource_specs.size() > 1) {
    for (size_t i = 1; i < datasource_specs.size(); ++i) {
      if (datasource_specs[i].id() != datasource_specs[0].id()) {
        YACL_THROW(
            "RunSQL operator could not handle query across datasource, "
            "table_refs=[{}]",
            fmt::join(table_refs, ","));
      }
    }
  }
  auto adaptor =
      ctx->GetDatasourceAdaptorMgr()->GetAdaptor(datasource_specs[0]);
  YACL_ENFORCE(adaptor, "get adaptor failed");

  const auto& outputs_pb = ctx->GetOutput(kOut);
  YACL_ENFORCE(outputs_pb.size() > 0, "no output for RunSQL");

  std::vector<ColumnDesc> expected_outputs;
  for (int i = 0; i < outputs_pb.size(); ++i) {
    expected_outputs.emplace_back(outputs_pb[i].name(),
                                  outputs_pb[i].elem_type());
  }

  TensorBuildOptions options = {
      .dump_to_disk = ctx->GetSession()->GetStreamingOptions().batched,
      .dump_dir = ctx->GetSession()->GetStreamingOptions().dump_file_dir};
  auto results = adaptor->ExecQuery(logger, select, expected_outputs, options);

  YACL_ENFORCE(results.size() == expected_outputs.size(),
               "the size of ExecQuery results mismatch with expected_outputs");
  for (size_t i = 0; i < expected_outputs.size(); ++i) {
    ctx->GetTensorTable()->AddTensor(expected_outputs[i].name, results[i]);
  }
  SPDLOG_LOGGER_INFO(logger, "get result row={}, column={}",
                     results[0]->Length(), results.size());

  audit::RecordSqlNodeDetail(*ctx, select, results[0]->Length(), results.size(),
                             start_time);
}

}  // namespace scql::engine::op