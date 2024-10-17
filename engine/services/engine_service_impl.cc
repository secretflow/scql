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

#include "engine/services/engine_service_impl.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <memory>
#include <stdexcept>
#include <utility>

#include "brpc/channel.h"
#include "brpc/closure_guard.h"
#include "google/protobuf/util/json_util.h"
#include "psi/utils/sync.h"

#include "engine/audit/audit_log.h"
#include "engine/framework/exec.h"
#include "engine/framework/executor.h"
#include "engine/framework/session.h"
#include "engine/operator/all_ops_register.h"
#include "engine/services/pipeline.h"
#include "engine/util/logging.h"
#include "engine/util/tensor_util.h"

#include "engine/services/error_collector_service.pb.h"

#ifndef LOG_ERROR_AND_SET_STATUS_IMPL
#define LOG_ERROR_AND_SET_STATUS_IMPL(logger, status, err_code, err_msg) \
  do {                                                                   \
    SPDLOG_LOGGER_ERROR(logger, err_msg);                                \
    status->set_code(err_code);                                          \
    status->set_message(err_msg);                                        \
  } while (false)
#endif  // LOG_ERROR_AND_SET_STATUS_IMPL

#ifndef LOG_ERROR_AND_SET_STATUS
#define LOG_ERROR_AND_SET_STATUS(status, err_code, err_msg)                   \
  do {                                                                        \
    LOG_ERROR_AND_SET_STATUS_IMPL(spdlog::default_logger(), status, err_code, \
                                  err_msg);                                   \
  } while (false)
#endif  // LOG_ERROR_AND_SET_STATUS

namespace {

std::string MessageToJsonString(const google::protobuf::Message& message) {
  ::google::protobuf::util::JsonOptions opts;
  opts.add_whitespace = false;
  opts.preserve_proto_field_names = true;
  opts.always_print_primitive_fields = true;
  opts.always_print_enums_as_ints = true;
  std::string result;
  ::google::protobuf::util::MessageToJsonString(message, &result, opts);
  return result;
}

void MergePeerErrors(
    const std::vector<std::pair<std::string, scql::pb::Status>>& peer_errors,
    scql::pb::Status* status) {
  std::string merged_error_msg{};
  for (const auto& error : peer_errors) {
    // NOTE: status->add_details() is not used here because the format used by
    // brpc to convert messages containing Any into json is not compatible
    // with the standard.
    merged_error_msg.append(fmt::format("; (peer: {}, code: {}, msg: {})",
                                        error.first, error.second.code(),
                                        error.second.message()));
  }
  status->set_message(fmt::format("{}{}", status->message(), merged_error_msg));
}

class CredentialExpception : std::runtime_error {
 public:
  explicit CredentialExpception(const std::string& msg)
      : std::runtime_error(
            fmt::format("[Driver credential check failed] {}", msg)) {}
};

}  // namespace

namespace scql::engine {

namespace {

int32_t CountTotalNodes(const scql::pb::SchedulingPolicy& policy) {
  int32_t nodes_count = 0;
  for (const auto& pipeline : policy.pipelines()) {
    for (const auto& subdag : pipeline.subdags()) {
      for (const auto& job : subdag.jobs()) {
        nodes_count += job.node_ids().size();
      }
    }
  }
  return nodes_count;
}

}  // namespace

EngineServiceImpl::EngineServiceImpl(
    const EngineServiceOptions& options,
    std::unique_ptr<SessionManager> session_mgr,
    ChannelManager* channel_manager,
    std::unique_ptr<auth::Authenticator> authenticator)
    : service_options_(options),
      session_mgr_(std::move(session_mgr)),
      channel_manager_(channel_manager),
      authenticator_(std::move(authenticator)) {
  if (options.enable_authorization && options.credential.empty()) {
    YACL_THROW(
        "credential is empty, you should provide credential for driver "
        "authorization");
  }
  op::RegisterAllOps();
}

void EngineServiceImpl::CheckDriverCredential(
    const brpc::HttpHeader& http_header) {
  if (!service_options_.enable_authorization) {
    return;
  }

  const std::string* credential = http_header.GetHeader("Credential");

  if (credential == nullptr || credential->empty()) {
    throw CredentialExpception("credential in http header is empty");
  }

  if (*credential != service_options_.credential) {
    throw CredentialExpception(
        "driver authorization failed, unknown driver credential");
  }
}

void EngineServiceImpl::StopJob(::google::protobuf::RpcController* cntl,
                                const pb::StopJobRequest* request,
                                pb::Status* status,
                                ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  // check illegal request.
  auto controller = static_cast<brpc::Controller*>(cntl);
  std::string source_ip =
      butil::endpoint2str(controller->remote_side()).c_str();
  auto session_id = request->job_id();
  try {
    CheckDriverCredential(controller->http_request());
  } catch (const std::exception& e) {
    LOG_ERROR_AND_SET_STATUS(status, pb::Code::UNAUTHENTICATED, e.what());
    audit::RecordUncategorizedEvent(*status, session_id, source_ip, "StopJob");
    return;
  }

  if (session_id.empty()) {
    std::string err_msg = "session_id in request is empty";
    LOG_ERROR_AND_SET_STATUS(status, pb::Code::INVALID_ARGUMENT, err_msg);
    audit::RecordUncategorizedEvent(*status, session_id, source_ip, "StopJob");
    return;
  }

  auto logger = GetActiveLogger(session_id);
  SPDLOG_LOGGER_INFO(logger, "EngineServiceImpl::StopJob({}), reason({})",
                     session_id, request->reason());

  try {
    session_mgr_->StopSession(session_id);

    status->set_code(pb::Code::OK);
    audit::RecordStopJobEvent(*request, *status, source_ip);
    return;
  } catch (const std::exception& e) {
    std::string err_msg =
        fmt::format("StopSession({}) failed, catch std::exception={}",
                    session_id, e.what());
    LOG_ERROR_AND_SET_STATUS_IMPL(logger, status,
                                  pb::Code::UNKNOWN_ENGINE_ERROR, err_msg);
    audit::RecordStopJobEvent(*request, *status, source_ip);
    return;
  }
}

void CheckGraphChecksum(const pb::RunExecutionPlanRequest*& request,
                        Session* session) {
  auto logger = (session != nullptr && session->GetLogger() != nullptr)
                    ? session->GetLogger()
                    : spdlog::default_logger();

  if (session->GetLink()->WorldSize() < 2 ||
      !request->graph_checksum().check_graph_checksum()) {
    SPDLOG_LOGGER_INFO(logger, "skip checking graph checksum");
    return;
  }

  SPDLOG_LOGGER_INFO(logger, "check graph checksum");
  const auto& graph_checksum = request->graph_checksum();
  auto iter = graph_checksum.sub_graph_checksums().find(
      std::to_string(session->SelfRank()));
  YACL_ENFORCE(iter != graph_checksum.sub_graph_checksums().end(),
               "get graph checksum failed for rank {}", session->SelfRank());
  const auto& whole_graph_checksum = graph_checksum.whole_graph_checksum();
  // concat whole graph checksum and self sub graph checksum
  auto checksum = whole_graph_checksum + iter->second;

  // get checksum from other parties
  auto checksum_bufs = yacl::link::AllGather(
      session->GetLink(),
      yacl::ByteContainerView(checksum.data(), checksum.length()),
      "graph checksum");

  for (size_t rank = 0; rank < session->GetLink()->WorldSize(); rank++) {
    if (rank == session->SelfRank()) {
      continue;
    }
    // split checksum
    auto peer_checksum = std::string_view(checksum_bufs[rank]);
    auto peer_whole_graph_checksum =
        peer_checksum.substr(0, whole_graph_checksum.size());
    YACL_ENFORCE(peer_whole_graph_checksum == whole_graph_checksum,
                 "peer_whole_graph_checksum: {} != whole_graph_checksum: {}",
                 peer_checksum, whole_graph_checksum);
    auto iter = graph_checksum.sub_graph_checksums().find(std::to_string(rank));
    YACL_ENFORCE(iter != graph_checksum.sub_graph_checksums().end(),
                 "get graph checksum failed for rank {}", session->SelfRank());
    auto local_sub_graph_checksum = std::string(iter->second);
    auto peer_sub_graph_checksum = peer_checksum.substr(
        whole_graph_checksum.size(), local_sub_graph_checksum.size());
    YACL_ENFORCE(peer_sub_graph_checksum == local_sub_graph_checksum,
                 "peer_checksum: {} != local_sub_graph_checksum: {}",
                 peer_sub_graph_checksum, local_sub_graph_checksum);
  }
}

void EngineServiceImpl::RunExecutionPlan(
    ::google::protobuf::RpcController* cntl,
    const pb::RunExecutionPlanRequest* request,
    pb::RunExecutionPlanResponse* response, ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto start_time = std::chrono::system_clock::now();
  auto controller = static_cast<brpc::Controller*>(cntl);
  std::string source_ip =
      butil::endpoint2str(controller->remote_side()).c_str();

  try {
    CheckDriverCredential(controller->http_request());
  } catch (const std::exception& e) {
    LOG_ERROR_AND_SET_STATUS(response->mutable_status(),
                             pb::Code::UNAUTHENTICATED, e.what());
    audit::RecordUncategorizedEvent(response->status(),
                                    request->job_params().job_id(), source_ip,
                                    "RunExecutionPlan");
    return;
  }

  const std::string session_id = request->job_params().job_id();
  response->set_job_id(session_id);
  response->set_party_code(request->job_params().party_code());

  if (request->async() && request->callback_url().empty()) {
    std::string err_msg = fmt::format(
        "RunExecutionPlan run jobs({}) failed, in async mode but "
        "callback_url is empty",
        session_id);
    LOG_ERROR_AND_SET_STATUS(response->mutable_status(), pb::Code::BAD_REQUEST,
                             err_msg);
    audit::RecordRunExecPlanEvent(*request, *response, start_time, source_ip);

    ReportErrorToPeers(request->job_params(), pb::Code::BAD_REQUEST, err_msg);
    return;
  }

  Session* session = nullptr;
  // 1. create session first.
  try {
    VerifyPublicKeys(request->job_params());

    session_mgr_->CreateSession(request->job_params(), request->debug_opts());
    session = session_mgr_->GetSession(session_id);
    YACL_ENFORCE(session, "get session failed");
    // check graph checksum
    CheckGraphChecksum(request, session);
    ::scql::pb::Status status;
    status.set_code(0);
    audit::RecordCreateSessionEvent(status, request->job_params(), true,
                                    source_ip);
  } catch (const std::exception& e) {
    std::string err_msg = fmt::format(
        "RunExecutionPlan create session({}) failed, catch "
        "std::exception={} ",
        session_id, e.what());
    LOG_ERROR_AND_SET_STATUS(response->mutable_status(),
                             pb::Code::UNKNOWN_ENGINE_ERROR, err_msg);
    audit::RecordCreateSessionEvent(response->status(), request->job_params(),
                                    true, source_ip);
    ReportErrorToPeers(request->job_params(), pb::Code::UNKNOWN_ENGINE_ERROR,
                       err_msg);
    return;
  }

  // 2. run plan in async or sync mode
  auto logger = ActiveLogger(session);
  SPDLOG_LOGGER_INFO(logger,
                     "[RunExecutionPlan] Session created successfully, start "
                     "to run plan, client = {}\nRunExecutionPlanRequest = {}",
                     source_ip, MessageToJsonString(*request));
  if (request->async()) {
    // Copy 'request' to avoid deconstruction before async call finished.
    worker_pool_.Submit(&EngineServiceImpl::RunPlanAsync, this, *request,
                        session, source_ip);
    SPDLOG_LOGGER_INFO(logger,
                       "submit runplan for session({}), dag queue length={}",
                       session_id, worker_pool_.GetQueueLength());
    response->mutable_status()->set_code(pb::Code::OK);
    return;
  } else {
    // run in sync mode
    RunPlanSync(request, session, response);
    audit::RecordRunExecPlanEvent(*request, *response, start_time, source_ip);

    try {
      session_mgr_->RemoveSession(session_id);
    } catch (const std::exception& e) {
      SPDLOG_LOGGER_ERROR(logger,
                          "RunExecutionPlan remove session({}) failed, catch "
                          "std::exception={}",
                          session_id, e.what());
    }
    return;
  }
}

void EngineServiceImpl::QueryJobStatus(::google::protobuf::RpcController* cntl,
                                       const pb::QueryJobStatusRequest* request,
                                       pb::QueryJobStatusResponse* response,
                                       ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* controller = static_cast<brpc::Controller*>(cntl);

  try {
    CheckDriverCredential(controller->http_request());
  } catch (const std::exception& e) {
    LOG_ERROR_AND_SET_STATUS(response->mutable_status(),
                             pb::Code::UNAUTHENTICATED, e.what());
    return;
  }

  const auto& job_id = request->job_id();
  if (job_id.empty()) {
    std::string err_msg = "bad request: job_id is empty";
    LOG_ERROR_AND_SET_STATUS(response->mutable_status(),
                             pb::Code::INVALID_ARGUMENT, err_msg);
    return;
  }
  auto* session = session_mgr_->GetSession(job_id);

  if (session == nullptr) {
    std::string err_msg =
        fmt::format("QueryJobStatus failed, job({}) not found", job_id);
    LOG_ERROR_AND_SET_STATUS(response->mutable_status(), pb::Code::NOT_FOUND,
                             err_msg);
    return;
  }

  auto logger = ActiveLogger(session);
  logger->debug("Session is valid, handle QueryJobStatus request, request={}",
                MessageToJsonString(*request));

  auto job_state = ConvertSessionStateToJobState(session->GetState());
  if (job_state == pb::JOB_STATE_UNSPECIFIED) {
    SPDLOG_LOGGER_WARN(logger, "unknown job state, job_id={}", job_id);
  }

  response->mutable_status()->set_code(pb::Code::OK);
  response->set_job_state(job_state);

  {
    // set job progress
    auto* progress = response->mutable_progress();

    auto chronoTimePointToTimestamp =
        [](const std::chrono::system_clock::time_point& tp)
        -> google::protobuf::Timestamp {
      google::protobuf::Timestamp timestamp;
      auto seconds = std::chrono::time_point_cast<std::chrono::seconds>(tp);
      auto nanos =
          std::chrono::time_point_cast<std::chrono::nanoseconds>(tp) -
          std::chrono::time_point_cast<std::chrono::nanoseconds>(seconds);
      timestamp.set_seconds(seconds.time_since_epoch().count());
      timestamp.set_nanos(static_cast<int>(nanos.count()));
      return timestamp;
    };
    progress->mutable_start_time()->CopyFrom(
        chronoTimePointToTimestamp(session->GetStartTime()));

    progress->set_stages_count(session->GetNodesCount());
    progress->set_executed_stages(session->GetExecutedNodes());

    auto link_stats = session->GetLinkStats();
    progress->mutable_io_stats()->set_send_bytes(link_stats->sent_bytes);
    progress->mutable_io_stats()->set_recv_bytes(link_stats->recv_bytes);
    progress->mutable_io_stats()->set_send_actions(link_stats->sent_actions);
    progress->mutable_io_stats()->set_recv_actions(link_stats->recv_actions);

    auto* current_stage = progress->add_running_stages();
    auto [node_start_time, current_node_name] = session->GetCurrentNodeInfo();
    current_stage->mutable_start_time()->CopyFrom(
        chronoTimePointToTimestamp(node_start_time));
    current_stage->set_name(current_node_name);
  }

  SPDLOG_LOGGER_INFO(
      logger, "Finish to handle QueryJobStatus request, job_id={}", job_id);
}

void EngineServiceImpl::ReportResult(const std::string& session_id,
                                     const std::string& cb_url,
                                     const std::string& report_info_str) {
  auto logger = GetActiveLogger(session_id);
  try {
    auto rpc_channel =
        channel_manager_->Create(logger, cb_url, RemoteRole::Driver);
    if (!rpc_channel) {
      SPDLOG_LOGGER_WARN(logger,
                         "create rpc channel failed for driver: session_id={}, "
                         "callback_url={}",
                         session_id, cb_url);
      return;
    }

    // do http report
    brpc::Controller cntl;
    if (cntl.http_request().uri().SetHttpURL(cb_url) != 0) {
      const auto& st = cntl.http_request().uri().status();
      SPDLOG_LOGGER_WARN(logger, "failed to set request URL: {}",
                         st.error_str());
      return;
    }
    cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
    cntl.http_request().set_content_type("application/json");
    cntl.request_attachment().append(report_info_str);

    SPDLOG_LOGGER_WARN(
        logger, "session({}) send rpc request to driver, callback_url={}",
        session_id, cb_url);
    // Because `done'(last parameter) is NULL, this function waits until
    // the response comes back or error occurs(including timeout).
    rpc_channel->CallMethod(nullptr, &cntl, nullptr, nullptr, nullptr);
    if (cntl.Failed()) {
      SPDLOG_LOGGER_WARN(
          logger, "session({}) report callback URL({}) failed with error({})",
          session_id, cb_url, cntl.ErrorText());
      return;
    }

    SPDLOG_LOGGER_INFO(
        logger, "session({}) report success, get response ({}), used({}ms)",
        session_id, cntl.response_attachment().to_string(),
        cntl.latency_us() / 1000);

  } catch (const std::exception& e) {
    SPDLOG_LOGGER_WARN(logger,
                       "ReportResult({}) failed, catch std::exception={}",
                       session_id, e.what());
    return;
  }
}

void EngineServiceImpl::RunPlanCore(const pb::RunExecutionPlanRequest& request,
                                    Session* session,
                                    pb::RunExecutionPlanResponse* response) {
  auto logger = ActiveLogger(session);
  const auto& graph = request.graph();
  const auto& policy = graph.policy();
  session->SetNodesCount(CountTotalNodes(policy));
  session->SetExecutedNodes(0);
  if (policy.pipelines().size() > 1) {
    session->EnableStreamingBatched();
  }
  for (int pipe_index = 0; pipe_index < policy.pipelines().size();
       pipe_index++) {
    const auto& pipeline = policy.pipelines()[pipe_index];
    PipelineExecutor pipe_executor(pipeline, session);
    for (size_t i = 0; i < pipe_executor.GetBatchNum(); i++) {
      SPDLOG_LOGGER_INFO(logger,
                         "session({}) start to execute pipeline({}) batch({})",
                         session->Id(), pipe_index, i);
      pipe_executor.UpdateTensorTable();
      for (const auto& subdag : pipeline.subdags()) {
        for (const auto& job : subdag.jobs()) {
          const auto& node_ids = job.node_ids();
          for (const auto& node_id : node_ids) {
            const auto& iter = graph.nodes().find(node_id);
            YACL_ENFORCE(iter != graph.nodes().cend(),
                         "no node for node_id={} in node_ids", node_id);
            const auto& node = iter->second;
            SPDLOG_LOGGER_INFO(logger,
                               "session({}) start to execute node({}) op({}) "
                               "pipeline({}) batch({})",
                               session->Id(), node.node_name(), node.op_type(),
                               pipe_index, i);
            auto start = std::chrono::system_clock::now();
            session->SetCurrentNodeInfo(start, node.node_name());

            YACL_ENFORCE(session->GetState() == SessionState::RUNNING,
                         "session status not equal to running");
            ExecContext context(node, session);
            Executor executor;
            executor.RunExecNode(&context);

            auto end = std::chrono::system_clock::now();
            SPDLOG_LOGGER_INFO(
                logger,
                "session({}) finished executing node({}), op({}), cost({})ms",
                session->Id(), node.node_name(), node.op_type(),
                std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                      start)
                    .count());
            // TODO(xiaoyuan): fix progress in streaming mode later
            session->IncExecutedNodes();
            if (!session->GetStreamingOptions().batched) {
              YACL_ENFORCE(
                  session->GetExecutedNodes() <= session->GetNodesCount(),
                  "executed nodes: {}, total nodes count: {}",
                  session->GetExecutedNodes(), session->GetNodesCount());
            }
            if (node.op_type() == "Publish") {
              auto results = session->GetPublishResults();
              for (const auto& result : results) {
                pb::Tensor* out_column = response->add_out_columns();
                out_column->CopyFrom(*result);
              }
            } else if (node.op_type() == "DumpFile" ||
                       node.op_type() == "InsertTable") {
              auto affected_rows = session->GetAffectedRows();
              response->set_num_rows_affected(affected_rows);
            }
          }
        }
        if (subdag.need_call_barrier_after_jobs()) {
          yacl::link::Barrier(session->GetLink(), session->Id());
        }
      }
      pipe_executor.FetchOutputTensors();
      SPDLOG_LOGGER_INFO(
          logger, "session({}) finished executing pipeline({}) batch({})",
          session->Id(), pipe_index, i);
    }
    pipe_executor.Finish();
  }
  SPDLOG_LOGGER_INFO(logger, "session({}) run plan policy succ", session->Id());
  response->mutable_status()->set_code(pb::Code::OK);
}

void EngineServiceImpl::VerifyPublicKeys(
    const pb::JobStartParams& start_params) {
  if (!authenticator_) {
    return;
  }
  std::vector<auth::PartyIdentity> parties;
  for (const auto& party : start_params.parties()) {
    parties.emplace_back(auth::PartyIdentity{party.code(), party.public_key()});
  }
  authenticator_->Verify(start_params.party_code(), parties);
}

void EngineServiceImpl::RunPlanSync(const pb::RunExecutionPlanRequest* request,
                                    Session* session,
                                    pb::RunExecutionPlanResponse* response) {
  auto logger = ActiveLogger(session);
  // 1. run jobs in plan and add result to response.
  const std::string session_id = request->job_params().job_id();
  try {
    auto run_f = std::async([&] {
      session->SetState(SessionState::RUNNING);

      RunPlanCore(*request, session, response);

      session->SetState(SessionState::SUCCEEDED);
    });
    // wait until all peers finished, if any one of them broken, the
    // ::yacl::IoError will be throw in other peers after Channel::Recv timeout
    ::psi::SyncWait(session->GetLink(), &run_f);

    SPDLOG_LOGGER_INFO(logger, "RunExecutionPlan success, sessionID={}",
                       session_id);
    auto lctx = session->GetLink();
    lctx->WaitLinkTaskFinish();
  } catch (const std::exception& e) {
    session->SetState(SessionState::FAILED);

    std::string err_msg = fmt::format(
        "RunExecutionPlan run jobs({}) failed, catch std::exception={}",
        session_id, e.what());
    LOG_ERROR_AND_SET_STATUS_IMPL(logger, response->mutable_status(),
                                  pb::Code::UNKNOWN_ENGINE_ERROR, err_msg);
    response->mutable_out_columns()->Clear();
    ReportErrorToPeers(request->job_params(), pb::Code::UNKNOWN_ENGINE_ERROR,
                       err_msg);
  }

  MergePeerErrors(session->GetPeerErrors(), response->mutable_status());
}

void EngineServiceImpl::RunPlanAsync(const pb::RunExecutionPlanRequest request,
                                     Session* session,
                                     const std::string& source_ip) {
  auto start_time = std::chrono::system_clock::now();

  pb::RunExecutionPlanResponse response;
  response.set_job_id(request.job_params().job_id());
  response.set_party_code(request.job_params().party_code());

  RunPlanSync(&request, session, &response);
  audit::RecordRunExecPlanEvent(request, response, start_time, source_ip);

  // prepare report info
  pb::ReportRequest report;
  report.mutable_status()->CopyFrom(response.status());
  report.mutable_out_columns()->CopyFrom(response.out_columns());
  report.set_job_id(response.job_id());
  report.set_party_code(response.party_code());
  report.set_num_rows_affected(response.num_rows_affected());

  ReportResult(request.job_params().job_id(), request.callback_url(),
               MessageToJsonString(report));

  // remove session.
  // NOTE: removing the session should be performed after ReportResult to
  // prevent the job watcher from receiving a "session not found" error when
  // executing QueryJobStatus.
  try {
    session_mgr_->RemoveSession(request.job_params().job_id());
  } catch (const std::exception& e) {
    SPDLOG_ERROR(
        "RunExecutionPlan remove session({}) failed, catch "
        "std::exception={}",
        request.job_params().job_id(), e.what());
  }
}

void EngineServiceImpl::ReportErrorToPeers(const pb::JobStartParams& params,
                                           const pb::Code err_code,
                                           const std::string& err_msg) {
  auto logger = GetActiveLogger(params.job_id());
  try {
    PartyInfo parties(params);
    for (const auto& party : parties.AllParties()) {
      if (party.id == parties.SelfPartyCode()) {
        continue;
      }

      auto rpc_channel =
          channel_manager_->Create(logger, party.host, RemoteRole::PeerEngine);
      YACL_ENFORCE(rpc_channel, "create rpc channel failed for party=({},{})",
                   party.id, party.host);

      services::pb::ReportErrorRequest request;
      services::pb::ReportErrorResponse response;
      brpc::Controller cntl;
      services::pb::ErrorCollectorService_Stub stub(rpc_channel.get());

      request.set_job_id(params.job_id());
      request.set_party_code(parties.SelfPartyCode());
      request.mutable_status()->set_code(err_code);
      request.mutable_status()->set_message(err_msg);

      stub.ReportError(&cntl, &request, &response, nullptr);
      if (cntl.Failed()) {
        SPDLOG_LOGGER_WARN(
            logger,
            "sync error to peer=({},{}) rpc failed: error_code: {}, "
            "error_text: {}",
            party.id, party.host, cntl.ErrorCode(), cntl.ErrorText());
      } else {
        if (response.status().code() != pb::Code::OK) {
          SPDLOG_LOGGER_WARN(
              logger, "sync error to peer=({},{}) failed: status: {}", party.id,
              party.host, response.status().DebugString());
        }
      }
    }
  } catch (const std::exception& e) {
    SPDLOG_LOGGER_WARN(logger, "sync error to peers failed: throw: {}",
                       e.what());
  }
}

std::shared_ptr<spdlog::logger> EngineServiceImpl::GetActiveLogger(
    const std::string& session_id) const {
  auto* session = session_mgr_->GetSession(session_id);
  return ActiveLogger(session);
}

}  // namespace scql::engine