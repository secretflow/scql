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

#include <cstddef>
#include <string>

#include "spdlog/spdlog.h"

#include "engine/framework/exec.h"

#include "api/engine.pb.h"
#include "engine/audit/audit.pb.h"
namespace scql::engine::audit {

using TimePoint = std::chrono::system_clock::time_point;

struct AuditOptions {
  std::string audit_log_file = "audit/audit.log";
  std::string audit_detail_file = "audit/detail.log";
  uint16_t audit_max_files = 180;
};

// Setup audit logger
void SetupAudit(const AuditOptions& opts = AuditOptions());

void RecordAudit(const AuditLog& audit_log);

void RecordUncategorizedEvent(const ::scql::pb::Status& status,
                              std::string session_id, std::string source_ip,
                              std::string url_path);

void RecordRunExecPlanEvent(const pb::RunExecutionPlanRequest& request,
                            const pb::RunExecutionPlanResponse& response,
                            const TimePoint& start_time, std::string source_ip);

void RecordCreateSessionEvent(const ::scql::pb::Status& status,
                              const pb::JobStartParams& params, bool sync,
                              std::string source_ip);

void RecordStopJobEvent(const pb::StopJobRequest& request,
                        const ::scql::pb::Status& status,
                        std::string source_ip);

void RecordDumpFileNodeDetail(const ExecContext& ctx,
                              const std::string& file_path,
                              const TimePoint& start_time);

void RecordJoinNodeDetail(const ExecContext& ctx, int64_t self_size,
                          int64_t peer_size, int64_t result_size,
                          const TimePoint& start_time);

void RecordInNodeDetail(const ExecContext& ctx, int64_t self_size,
                        int64_t peer_size, int64_t result_size,
                        const TimePoint& start_time);

void RecordSqlNodeDetail(const ExecContext& ctx, std::string query,
                         int64_t num_rows, int64_t num_columns,
                         const TimePoint& start_time);

void RecordPublishNodeDetail(const ExecContext& ctx, int64_t num_rows,
                             const TimePoint& start_time);
}  // namespace scql::engine::audit