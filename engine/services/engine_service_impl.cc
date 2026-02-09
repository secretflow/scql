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
#include <cstring>
#include <exception>
#include <memory>
#include <stdexcept>
#include <utility>

#include "brpc/closure_guard.h"
#include "google/protobuf/util/json_util.h"
#include "psi/utils/sync.h"

#include "engine/exe/flags.h"
#include "engine/framework/exec.h"
#include "engine/framework/executor.h"
#include "engine/framework/session.h"
#include "engine/operator/all_ops_register.h"
#include "engine/services/pipeline.h"
#include "engine/util/kpad_task_helper.h"
#include "engine/util/trace_categories.h"

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
  TRACE_EVENT_DEFAULT_TRACK(RPCCALL_CATEGORY, "EngineServiceImpl::StopJob");
  brpc::ClosureGuard done_guard(done);
  // check illegal request.
  auto* controller = static_cast<brpc::Controller*>(cntl);
  std::string source_ip =
      butil::endpoint2str(controller->remote_side()).c_str();
  auto session_id = request->job_id();
  try {
    CheckDriverCredential(controller->http_request());
  } catch (const std::exception& e) {
    LOG_ERROR_AND_SET_STATUS(status, pb::Code::UNAUTHENTICATED, e.what());
    return;
  }

  if (session_id.empty()) {
    std::string err_msg = "session_id in request is empty";
    LOG_ERROR_AND_SET_STATUS(status, pb::Code::INVALID_ARGUMENT, err_msg);
    return;
  }

  auto logger = GetActiveLogger(session_id);
  SPDLOG_LOGGER_INFO(logger, "EngineServiceImpl::StopJob({}), reason({})",
                     session_id, request->reason());

  try {
    session_mgr_->StopSession(session_id);

    status->set_code(pb::Code::OK);
    return;
  } catch (const std::exception& e) {
    std::string err_msg =
        fmt::format("StopSession({}) failed, catch std::exception={}",
                    session_id, e.what());
    LOG_ERROR_AND_SET_STATUS_IMPL(logger, status,
                                  pb::Code::UNKNOWN_ENGINE_ERROR, err_msg);
    return;
  }
}

void CheckGraphChecksum(const pb::RunExecutionPlanRequest* request,
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
    if (peer_sub_graph_checksum != local_sub_graph_checksum) {
      SPDLOG_LOGGER_WARN(logger,
                         "graph checksum mismatch for rank {}, self sub graph "
                         "checksum: {}, peer sub graph checksum: {}",
                         rank, local_sub_graph_checksum,
                         peer_sub_graph_checksum);
    }
  }
}

void EngineServiceImpl::RunExecutionPlan(
    ::google::protobuf::RpcController* cntl,
    const pb::RunExecutionPlanRequest* request,
    pb::RunExecutionPlanResponse* response, ::google::protobuf::Closure* done) {
  TRACE_EVENT_DEFAULT_TRACK(RPCCALL_CATEGORY,
                            "EngineServiceImpl::RunExecutionPlan");
  brpc::ClosureGuard done_guard(done);
  auto* controller = static_cast<brpc::Controller*>(cntl);
  std::string source_ip =
      butil::endpoint2str(controller->remote_side()).c_str();

  try {
    CheckDriverCredential(controller->http_request());
  } catch (const std::exception& e) {
    LOG_ERROR_AND_SET_STATUS(response->mutable_status(),
                             pb::Code::UNAUTHENTICATED, e.what());
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
  } catch (const std::exception& e) {
    std::string err_msg = fmt::format(
        "RunExecutionPlan create session({}) failed, catch "
        "std::exception={} ",
        session_id, e.what());
    LOG_ERROR_AND_SET_STATUS(response->mutable_status(),
                             pb::Code::UNKNOWN_ENGINE_ERROR, err_msg);
    ReportErrorToPeers(request->job_params(), pb::Code::UNKNOWN_ENGINE_ERROR,
                       err_msg);
    return;
  }
  // report communication data during initialization
  COMMUNICATION_COUNTER(session->GetLinkStats());
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
    // Cannot directly set session status because the session may have already
    // been removed
    session_mgr_->CompareAndSetState(session_id, SessionState::COMP_FINISHED,
                                     SessionState::SUCCEEDED);

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

    progress->set_stages_count(session->GetProgressStats()->GetNodesCnt());
    progress->set_executed_stages(
        session->GetProgressStats()->GetExecutedNodes());

    auto link_stats = session->GetLinkStats();
    progress->mutable_io_stats()->set_send_bytes(link_stats->sent_bytes);
    progress->mutable_io_stats()->set_recv_bytes(link_stats->recv_bytes);
    progress->mutable_io_stats()->set_send_actions(link_stats->sent_actions);
    progress->mutable_io_stats()->set_recv_actions(link_stats->recv_actions);
    auto* current_stage = progress->add_running_stages();
    auto [node_start_time, current_node_name] =
        session->GetProgressStats()->GetCurrentNodeInfo();
    current_stage->mutable_start_time()->CopyFrom(
        chronoTimePointToTimestamp(node_start_time));
    current_stage->set_name(current_node_name);
    current_stage->set_summary(session->GetProgressStats()->Summary());
  }

  SPDLOG_LOGGER_INFO(
      logger, "Finish to handle QueryJobStatus request, job_id={}", job_id);
}

void EngineServiceImpl::ReportResult(Session* session,
                                     const std::string& cb_url,
                                     const std::string& report_info_str) {
  auto session_id = session->Id();
  auto logger = GetActiveLogger(session_id);
  try {
    auto rpc_channel =
        channel_manager_->Create(logger, cb_url, RemoteRole::Driver);
    if (!rpc_channel) {
      session->SetState(SessionState::FAILED);
      SPDLOG_LOGGER_WARN(logger,
                         "create rpc channel failed for driver: session_id={}, "
                         "callback_url={}",
                         session_id, cb_url);
      return;
    }

    // do http report
    brpc::Controller cntl;
    if (cntl.http_request().uri().SetHttpURL(cb_url) != 0) {
      session->SetState(SessionState::FAILED);
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
      session->SetState(SessionState::FAILED);
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
    session->SetState(SessionState::FAILED);
    SPDLOG_LOGGER_WARN(logger,
                       "ReportResult({}) failed, catch std::exception={}",
                       session_id, e.what());
    return;
  }
}

void EngineServiceImpl::VerifyPublicKeys(
    const pb::JobStartParams& start_params) {
  TRACE_EVENT_DEFAULT_TRACK(OTHER_CATEGORY,
                            "EngineServiceImpl::VerifyPublicKeys");
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
  TRACE_EVENT_DEFAULT_TRACK(RPCCALL_CATEGORY, "EngineServiceImpl::RunPlanSync");
  auto logger = ActiveLogger(session);
  // 1. run jobs in plan and add result to response.
  const std::string session_id = request->job_params().job_id();
  try {
    // wait until all peers finished, if any one of them broken, the
    // ::yacl::IoError will be throw in other peers after Channel::Recv timeout
    ::psi::SyncWait(session->GetLink(), [&] {
      session->SetState(SessionState::RUNNING);
      ::scql::engine::RunPlanCore(*request, session, response);
    });
    session->SetState(SessionState::COMP_FINISHED);

    SPDLOG_LOGGER_INFO(logger, "RunExecutionPlan completed, sessionID={}",
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

void EngineServiceImpl::RunPlanAsync(const pb::RunExecutionPlanRequest& request,
                                     Session* session,
                                     const std::string& source_ip) {
  TRACE_EVENT_DEFAULT_TRACK(RPCCALL_CATEGORY,
                            "EngineServiceImpl::RunPlanASync");
  pb::RunExecutionPlanResponse response;
  response.set_job_id(request.job_params().job_id());
  response.set_party_code(request.job_params().party_code());

  auto session_id = session->Id();
  RunPlanSync(&request, session, &response);

  // prepare report info
  pb::ReportRequest report;
  report.mutable_status()->CopyFrom(response.status());
  report.mutable_out_columns()->CopyFrom(response.out_columns());
  report.set_job_id(response.job_id());
  report.set_party_code(response.party_code());
  report.set_num_rows_affected(response.num_rows_affected());

  ReportResult(session, request.callback_url(), MessageToJsonString(report));
  // Cannot directly set session status because the session may have already
  // been removed
  session_mgr_->CompareAndSetState(session_id, SessionState::COMP_FINISHED,
                                   SessionState::SUCCEEDED);

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
  TRACE_EVENT_DEFAULT_TRACK(RPCCALL_CATEGORY,
                            "EngineServiceImpl::ReportErrorToPeers");
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

void EngineServiceImpl::RunKpadTask(const util::ClusterDef& cluster_def) {
  SPDLOG_INFO("Starting kpad task execution");

  auto scql_config =
      scql::engine::util::ParseScqlConfig(FLAGS_kpad_scql_config);
  YACL_ENFORCE(scql_config,
               "Failed to parse kpad_scql_config or kpad_scql_config is empty");

  auto self_party = cluster_def.self_party;
  auto sub_graph_iter = scql_config->all_sub_graphs().find(self_party);
  YACL_ENFORCE(sub_graph_iter != scql_config->all_sub_graphs().end(),
               "No subgraph found for self party: {}", self_party);

  auto job_params = scql::engine::util::BuildJobStartParams(
      cluster_def, *scql_config, FLAGS_kpad_job_id);
  YACL_ENFORCE(job_params, "Failed to build job start parameters");

  // Create session
  session_mgr_->CreateSession(*job_params, pb::DebugOptions{});
  Session* session = session_mgr_->GetSession(FLAGS_kpad_job_id);
  YACL_ENFORCE(session, "Failed to get session");

  // Report communication data during initialization
  COMMUNICATION_COUNTER(session->GetLinkStats());

  auto logger = ActiveLogger(session);
  SPDLOG_LOGGER_INFO(logger,
                     "[RunKpadTaskMode] Session created successfully, start "
                     "to run kpad task, job_id={}",
                     FLAGS_kpad_job_id);

  // Build execution plan request
  pb::RunExecutionPlanRequest request;
  *request.mutable_job_params() = *job_params;
  *request.mutable_graph() = sub_graph_iter->second;

  // Run plan in sync mode
  pb::RunExecutionPlanResponse response;
  RunPlanSync(&request, session, &response);
  session_mgr_->CompareAndSetState(
      FLAGS_kpad_job_id, SessionState::COMP_FINISHED, SessionState::SUCCEEDED);

  session_mgr_->RemoveSession(FLAGS_kpad_job_id);

  YACL_ENFORCE(response.status().code() == pb::Code::OK,
               "Kpad task execution failed with status: {} {}",
               response.status().code(), response.status().message());

  SPDLOG_INFO("Kpad task execution completed successfully");
}

}  // namespace scql::engine
