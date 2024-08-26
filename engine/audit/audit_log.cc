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

#include "engine/audit/audit_log.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>

#include "butil/file_util.h"
#include "gflags/gflags.h"
#include "google/protobuf/util/json_util.h"
#include "spdlog/sinks/daily_file_sink.h"
#include "yacl/base/exception.h"
namespace scql::engine::audit {

const std::string default_logger_name = "audit";
const std::string detail_logger_name = "detail";

static bool audit_init_finished = false;

void InitAuditLogger(const AuditOptions& opts) {
  const butil::FilePath audit_log_dir{opts.audit_log_file};
  const butil::FilePath audit_detail_dir{opts.audit_detail_file};
  {
    butil::File::Error error;
    YACL_ENFORCE(butil::CreateDirectoryAndGetError(audit_log_dir, &error),
                 "Failed to create directory={}: {}", audit_log_dir.value(),
                 butil::File::ErrorToString(error));
    YACL_ENFORCE(butil::CreateDirectoryAndGetError(audit_detail_dir, &error),
                 "Failed to create directory={}: {}", audit_detail_dir.value(),
                 butil::File::ErrorToString(error));
  }

  // 1. setup audit logger link.
  // default_sink used to record default audit, detail_sink used to record
  // execution plan info and sub dag info
  auto default_sink = std::make_shared<spdlog::sinks::daily_file_sink_mt>(
      opts.audit_log_file, 0, 0, false, opts.audit_max_files);
  auto detail_sink = std::make_shared<spdlog::sinks::daily_file_sink_mt>(
      opts.audit_detail_file, 0, 0, false, opts.audit_max_files);

  // 2. register audit log
  auto audit_logger =
      std::make_shared<spdlog::logger>(default_logger_name, default_sink);
  auto plan_logger =
      std::make_shared<spdlog::logger>(detail_logger_name, detail_sink);

  audit_logger->set_level(spdlog::level::info);
  audit_logger->set_pattern("%v");
  audit_logger->flush_on(spdlog::level::info);

  plan_logger->set_level(spdlog::level::info);
  plan_logger->set_pattern("%v");
  plan_logger->flush_on(spdlog::level::info);

  spdlog::register_logger(audit_logger);

  spdlog::register_logger(plan_logger);
  audit_init_finished = true;
}

void SetupAudit(const AuditOptions& opts) {
  static std::once_flag flag;
  std::call_once(flag, InitAuditLogger, opts);
}

void RecordAudit(const AuditLog& audit_log) {
  if (!audit_init_finished) {
    return;
  }
  ::google::protobuf::util::JsonOptions opts;
  opts.add_whitespace = false;
  opts.preserve_proto_field_names = true;
  opts.always_print_primitive_fields = true;

  std::string json_str;
  auto status =
      ::google::protobuf::util::MessageToJsonString(audit_log, &json_str, opts);
  if (!status.ok()) {
    SPDLOG_WARN(
        "record audit failed to convert proto message to json string: {}",
        status.ToString());
    return;
  }

  auto audit_type = audit_log.body().body_case();
  switch (audit_type) {
    case AuditBody::kRunPlan:
    case AuditBody::kCreateSession:
    case AuditBody::kStopJob:
    case AuditBody::kUncategorized: {
      auto audit_logger = spdlog::get(default_logger_name);
      if (audit_logger == nullptr) {
        SPDLOG_WARN("record audit failed to get default logger");
        return;
      }
      audit_logger->warn(json_str);
      break;
    }
    default: {
      auto detail_logger = spdlog::get(detail_logger_name);
      if (detail_logger == nullptr) {
        SPDLOG_WARN("record audit failed to get detail logger");
        return;
      }
      detail_logger->warn(json_str);
      break;
    }
  }
}

::google::protobuf::Map<std::string, ::audit::pb::strings>
GetTensorInfoFromExecNode(
    const ::google::protobuf::Map<std::string, pb::TensorList>& tensor_list) {
  ::google::protobuf::Map<std::string, ::audit::pb::strings> result;
  for (const auto& iter : tensor_list) {
    auto tensors = iter.second.tensors();
    ::audit::pb::strings names;
    for (const auto& tensor : tensors) {
      names.add_ss(tensor.name());
    }
    result[iter.first] = names;
  }
  return result;
}

::audit::pb::NodeInfo GetSimpleInfoFromExecNode(
    const ::scql::pb::ExecNode& exec_node) {
  ::audit::pb::NodeInfo node_info;
  node_info.set_name(exec_node.node_name());
  auto inputs_info = GetTensorInfoFromExecNode(exec_node.inputs());

  auto outputs_info = GetTensorInfoFromExecNode(exec_node.outputs());

  node_info.mutable_inputs()->swap(inputs_info);
  node_info.mutable_outputs()->swap(outputs_info);
  return node_info;
}

int64_t GetCostTime(const TimePoint& start_time) {
  const auto time_now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(time_now -
                                                               start_time)
      .count();
}

int64_t GetTimeSec(const TimePoint& start_time) {
  return std::chrono::duration_cast<std::chrono::seconds>(
             start_time.time_since_epoch())
      .count();
}

void RecordUncategorizedEvent(const ::scql::pb::Status& status,
                              std::string session_id, std::string source_ip,
                              std::string url_path) {
  if (!audit_init_finished) {
    return;
  }
  AuditLog audit;
  auto* header = audit.mutable_header();
  header->mutable_time()->set_seconds(
      GetTimeSec(std::chrono::system_clock::now()));
  header->mutable_status()->CopyFrom(status);
  header->set_event_name(::audit::pb::UNCATEGORIZED);
  header->set_session_id(session_id);
  auto* body = audit.mutable_body()->mutable_uncategorized();
  body->set_source_ip(source_ip);
  body->set_url_path(url_path);
  RecordAudit(audit);
}

void RecordRunExecPlanEvent(const pb::RunExecutionPlanRequest& request,
                            const pb::RunExecutionPlanResponse& response,
                            const TimePoint& start_time,
                            std::string source_ip) {
  if (!audit_init_finished) {
    return;
  }
  AuditLog audit;
  auto* header = audit.mutable_header();
  header->mutable_time()->set_seconds(GetTimeSec(start_time));
  header->mutable_status()->CopyFrom(response.status());
  header->set_event_name(::audit::pb::RUN_EXEC_PLAN);
  header->set_session_id(request.job_params().job_id());

  auto* body = audit.mutable_body()->mutable_run_plan();
  body->set_source_ip(source_ip);
  body->set_cost_time(GetCostTime(start_time));
  body->set_affected_rows(response.num_rows_affected());

  for (const auto& iter : request.graph().nodes()) {
    const auto& exec_node = iter.second;
    auto node_info = GetSimpleInfoFromExecNode(exec_node);
    auto* node_to_add = body->add_node_list();
    node_to_add->CopyFrom(node_info);
  }
  RecordAudit(audit);
}

void RecordCreateSessionEvent(const ::scql::pb::Status& status,
                              const pb::JobStartParams& params, bool sync,
                              std::string source_ip) {
  if (!audit_init_finished) {
    return;
  }
  AuditLog audit;
  auto* header = audit.mutable_header();
  header->mutable_time()->set_seconds(
      GetTimeSec(std::chrono::system_clock::now()));
  header->mutable_status()->CopyFrom(status);
  header->set_session_id(params.job_id());
  header->set_event_name(::audit::pb::CREATE_SESSION);

  auto* body = audit.mutable_body()->mutable_create_session();
  body->set_source_ip(source_ip);
  body->set_sync(sync);
  body->mutable_parameters()->CopyFrom(params);
  RecordAudit(audit);
}

void RecordStopJobEvent(const pb::StopJobRequest& request,
                        const ::scql::pb::Status& status,
                        std::string source_ip) {
  if (!audit_init_finished) {
    return;
  }
  AuditLog audit;
  auto* header = audit.mutable_header();
  header->mutable_time()->set_seconds(
      GetTimeSec(std::chrono::system_clock::now()));
  header->mutable_status()->CopyFrom(status);
  header->set_session_id(request.job_id());
  header->set_event_name(::audit::pb::STOP_JOB);

  auto* body = audit.mutable_body()->mutable_stop_job();
  body->set_reason(request.reason());
  body->set_source_ip(source_ip);
  RecordAudit(audit);
}

void RecordDumpFileNodeDetail(const ExecContext& ctx,
                              const std::string& file_path,
                              const TimePoint& start_time) {
  if (!audit_init_finished) {
    return;
  }
  AuditLog audit;
  auto* header = audit.mutable_header();
  header->mutable_time()->set_seconds(GetTimeSec(start_time));

  header->set_session_id(ctx.GetSession()->Id());
  header->set_event_name(::audit::pb::DUMP_FILE_DETAIL);
  auto* body = audit.mutable_body()->mutable_dump_detail();
  body->set_node_name(ctx.GetNodeName());
  body->set_file_path(file_path);
  body->set_affected_rows(ctx.GetSession()->GetAffectedRows());
  body->set_cost_time(GetCostTime(start_time));
  RecordAudit(audit);
}

void RecordJoinNodeDetail(const ExecContext& ctx, int64_t self_size,
                          int64_t peer_size, int64_t result_size,
                          const TimePoint& start_time) {
  if (!audit_init_finished) {
    return;
  }
  AuditLog audit;
  auto* header = audit.mutable_header();
  header->mutable_time()->set_seconds(GetTimeSec(start_time));

  header->set_session_id(ctx.GetSession()->Id());
  header->set_event_name(::audit::pb::JOIN_DETAIL);
  auto* body = audit.mutable_body()->mutable_join_detail();

  body->set_node_name(ctx.GetNodeName());
  body->set_self_party(ctx.GetSession()->SelfPartyCode());
  body->set_self_rank(static_cast<int64_t>(ctx.GetSession()->SelfRank()));
  body->set_self_size(self_size);
  body->set_peer_size(peer_size);
  body->set_result_size(result_size);

  body->set_cost_time(GetCostTime(start_time));

  std::vector<std::string> input_party_codes =
      ctx.GetStringValuesFromAttribute("input_party_codes");

  for (const auto& input_party_code : input_party_codes) {
    body->add_party_codes()->append(input_party_code);
  }
  RecordAudit(audit);
}

void RecordInNodeDetail(const ExecContext& ctx, int64_t self_size,
                        int64_t peer_size, int64_t result_size,
                        const TimePoint& start_time) {
  if (!audit_init_finished) {
    return;
  }

  AuditLog audit;
  auto* header = audit.mutable_header();
  header->mutable_time()->set_seconds(GetTimeSec(start_time));

  header->set_session_id(ctx.GetSession()->Id());
  header->set_event_name(::audit::pb::IN_DETAIL);
  auto* body = audit.mutable_body()->mutable_in_detail();
  body->set_cost_time(GetCostTime(start_time));
  body->set_node_name(ctx.GetNodeName());
  body->set_self_party(ctx.GetSession()->SelfPartyCode());
  body->set_self_size(self_size);
  body->set_peer_size(peer_size);
  body->set_result_size(result_size);

  std::string reveal_to = ctx.GetStringValueFromAttribute("reveal_to");
  body->set_reveal_to(reveal_to);

  std::vector<std::string> input_party_codes =
      ctx.GetStringValuesFromAttribute("input_party_codes");
  for (const auto& input_party_code : input_party_codes) {
    body->add_party_codes()->append(input_party_code);
  }
  RecordAudit(audit);
}

void RecordSqlNodeDetail(const ExecContext& ctx, std::string query,
                         int64_t num_rows, int64_t num_columns,
                         const TimePoint& start_time) {
  if (!audit_init_finished) {
    return;
  }
  AuditLog audit;
  auto* header = audit.mutable_header();
  header->mutable_time()->set_seconds(GetTimeSec(start_time));
  header->set_session_id(ctx.GetSession()->Id());
  header->set_event_name(::audit::pb::SQL_DETAIL);

  auto* body = audit.mutable_body()->mutable_sql_detail();
  body->set_cost_time(GetCostTime(start_time));
  body->set_node_name(ctx.GetNodeName());
  body->set_query(query);
  body->set_num_columns(num_columns);
  body->set_num_columns(num_rows);
  RecordAudit(audit);
}

void RecordPublishNodeDetail(const ExecContext& ctx, int64_t num_rows,
                             const TimePoint& start_time) {
  if (!audit_init_finished) {
    return;
  }
  AuditLog audit;
  auto* header = audit.mutable_header();
  header->mutable_time()->set_seconds(GetTimeSec(start_time));
  header->set_session_id(ctx.GetSession()->Id());
  header->set_event_name(::audit::pb::PUBLISH_DETAIL);

  auto* body = audit.mutable_body()->mutable_publish_detail();
  body->set_cost_time(GetCostTime(start_time));
  body->set_node_name(ctx.GetNodeName());
  body->set_num_rows(num_rows);
  RecordAudit(audit);
}

}  // namespace scql::engine::audit