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

#include <cstddef>
#include <utility>

#include "brpc/channel.h"
#include "brpc/closure_guard.h"
#include "google/protobuf/util/json_util.h"

#include "engine/audit/audit_log.h"
#include "engine/framework/exec.h"
#include "engine/framework/executor.h"
#include "engine/operator/all_ops_register.h"
#include "engine/util/tensor_util.h"

#include "api/engine.pb.h"
#include "api/status.pb.h"
#include "api/status_code.pb.h"
#include "engine/services/error_collector_service.pb.h"

#ifndef LOG_ERROR_AND_SET_STATUS
#define LOG_ERROR_AND_SET_STATUS(status, err_code, err_msg) \
  do {                                                      \
    SPDLOG_ERROR(err_msg);                                  \
    status->set_code(err_code);                             \
    status->set_message(err_msg);                           \
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
  for (const auto& error : peer_errors) {
    // NOTE: status->add_details() is not used here because the format used by
    // brpc to convert messages containing Any into json is not compatible with
    // the standard.
    status->set_message(
        fmt::format("{}; (peer: {}, code: {}, msg: {})", status->message(),
                    error.first, error.second.code(), error.second.message()));
  }
}

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

bool EngineServiceImpl::CheckDriverCredential(
    const brpc::HttpHeader& http_header) {
  if (!service_options_.enable_authorization) {
    return true;
  }
  const std::string* credential = http_header.GetHeader("Credential");
  if (credential == nullptr || credential->empty()) {
    SPDLOG_ERROR("credential in http header is empty");
    return false;
  }

  if (*credential != service_options_.credential) {
    SPDLOG_ERROR("driver authorization failed, unknown driver credential");
    return false;
  }
  return true;
}

void EngineServiceImpl::StopSession(::google::protobuf::RpcController* cntl,
                                    const pb::StopSessionRequest* request,
                                    pb::Status* status,
                                    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  // check illegal request.
  auto controller = static_cast<brpc::Controller*>(cntl);
  std::string source_ip =
      butil::endpoint2str(controller->remote_side()).c_str();
  auto session_id = request->session_id();
  if (!CheckDriverCredential(controller->http_request())) {
    std::string err_msg = "driver authentication failed";
    LOG_ERROR_AND_SET_STATUS(status, pb::Code::UNAUTHENTICATED, err_msg);
    audit::RecordUncategorizedEvent(*status, session_id, source_ip,
                                    "StopSession");
    return;
  }
  if (session_id.empty()) {
    std::string err_msg = "session_id in request is empty";
    LOG_ERROR_AND_SET_STATUS(status, pb::Code::INVALID_ARGUMENT, err_msg);
    audit::RecordUncategorizedEvent(*status, session_id, source_ip,
                                    "StopSession");
    return;
  }
  SPDLOG_INFO("EngineServiceImpl::StopSession({}), reason({})", session_id,
              request->reason());
  try {
    session_mgr_->RemoveSession(session_id);

    status->set_code(pb::Code::OK);
    audit::RecordStopSessionEvent(*request, *status, source_ip);
    return;
  } catch (const std::exception& e) {
    std::string err_msg =
        fmt::format("StopSession({}) failed, catch std::exception={} ",
                    session_id, e.what());
    LOG_ERROR_AND_SET_STATUS(status, pb::Code::UNKNOWN_ENGINE_ERROR, err_msg);
    audit::RecordStopSessionEvent(*request, *status, source_ip);
    return;
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
  if (!CheckDriverCredential(controller->http_request())) {
    std::string err_msg = "driver authentication failed";
    LOG_ERROR_AND_SET_STATUS(response->mutable_status(),
                             pb::Code::UNAUTHENTICATED, err_msg);
    audit::RecordUncategorizedEvent(response->status(),
                                    request->session_params().session_id(),
                                    source_ip, "RunExecutionPlan");
    ReportErrorToPeers(request->session_params(), pb::Code::UNAUTHENTICATED,
                       err_msg);
    return;
  }

  const std::string session_id = request->session_params().session_id();
  response->set_session_id(session_id);
  response->set_party_code(request->session_params().party_code());

  if (request->async() && request->callback_url().empty()) {
    std::string err_msg = fmt::format(
        "RunExecutionPlan run jobs({}) failed, in async mode but "
        "callback_url is empty",
        session_id);
    LOG_ERROR_AND_SET_STATUS(response->mutable_status(), pb::Code::BAD_REQUEST,
                             err_msg);
    audit::RecordRunExecPlanEvent(*request, *response, start_time, source_ip);

    ReportErrorToPeers(request->session_params(), pb::Code::BAD_REQUEST,
                       err_msg);
    return;
  }

  Session* session = nullptr;
  // 1. create session first.
  try {
    VerifyPublicKeys(request->session_params());

    session_mgr_->CreateSession(request->session_params());
    session = session_mgr_->GetSession(session_id);
    YACL_ENFORCE(session, "get session failed");
    ::scql::pb::Status status;
    status.set_code(0);
    audit::RecordCreateSessionEvent(status, request->session_params(), true,
                                    source_ip);
  } catch (const std::exception& e) {
    std::string err_msg = fmt::format(
        "RunExecutionPlan create session({}) failed, catch "
        "std::exception={} ",
        session_id, e.what());
    LOG_ERROR_AND_SET_STATUS(response->mutable_status(),
                             pb::Code::UNKNOWN_ENGINE_ERROR, err_msg);
    audit::RecordCreateSessionEvent(response->status(),
                                    request->session_params(), true, source_ip);
    ReportErrorToPeers(request->session_params(),
                       pb::Code::UNKNOWN_ENGINE_ERROR, err_msg);
    return;
  }

  // 2. run plan in async or sync mode
  if (request->async()) {
    // Copy 'request' to avoid deconstruction before async call finished.
    worker_pool_.Submit(&EngineServiceImpl::RunPlanAsync, this, *request,
                        session, source_ip);
    SPDLOG_INFO("submit runplan for session({}), dag queue length={}",
                session_id, worker_pool_.GetQueueLength());
    response->mutable_status()->set_code(pb::Code::OK);
    return;
  } else {
    // run in sync mode
    RunPlanSync(request, session, response);
    audit::RecordRunExecPlanEvent(*request, *response, start_time, source_ip);
    return;
  }
}

void EngineServiceImpl::ReportResult(const std::string& session_id,
                                     const std::string& cb_url,
                                     const std::string& report_info_str) {
  try {
    auto rpc_channel = channel_manager_->Create(cb_url, RemoteRole::Driver);
    if (!rpc_channel) {
      SPDLOG_WARN(
          "create rpc channel failed for driver: session_id={}, "
          "callback_url={}",
          session_id, cb_url);
      return;
    }

    // do http report
    brpc::Controller cntl;
    if (cntl.http_request().uri().SetHttpURL(cb_url) != 0) {
      const auto& st = cntl.http_request().uri().status();
      SPDLOG_WARN("failed to set request URL: {}", st.error_str());
      return;
    }
    cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
    cntl.http_request().set_content_type("application/json");
    cntl.request_attachment().append(report_info_str);

    SPDLOG_INFO("session({}) send rpc request to driver, callback_url={}",
                session_id, cb_url);
    // Because `done'(last parameter) is NULL, this function waits until
    // the response comes back or error occurs(including timeout).
    rpc_channel->CallMethod(nullptr, &cntl, nullptr, nullptr, nullptr);
    if (cntl.Failed()) {
      SPDLOG_WARN("session({}) report callback URL({}) failed with error({})",
                  session_id, cb_url, cntl.ErrorText());
      return;
    }

    SPDLOG_INFO("session({}) report success, get response ({}), used({}ms)",
                session_id, cntl.response_attachment().to_string(),
                cntl.latency_us() / 1000);

  } catch (const std::exception& e) {
    SPDLOG_WARN("ReportResult({}) failed, catch std::exception={} ", session_id,
                e.what());
    return;
  }

  return;
}

void EngineServiceImpl::RunPlanCore(const pb::RunExecutionPlanRequest& request,
                                    Session* session,
                                    pb::RunExecutionPlanResponse* response) {
  const auto& graph = request.graph();
  const auto& policy = graph.policy();
  for (const auto& subdag : policy.subdags()) {
    for (const auto& job : subdag.jobs()) {
      const auto& node_ids = job.node_ids();
      for (const auto& node_id : node_ids) {
        const auto& iter = graph.nodes().find(node_id);
        YACL_ENFORCE(iter != graph.nodes().cend(),
                     "no node for node_id={} in node_ids", node_id);
        const auto& node = iter->second;
        SPDLOG_INFO("session({}) start to execute node({}), op({})",
                    session->Id(), node.node_name(), node.op_type());
        auto start = std::chrono::system_clock::now();

        ExecContext context(node, session);
        Executor executor;
        executor.RunExecNode(&context);

        auto end = std::chrono::system_clock::now();
        SPDLOG_INFO(
            "session({}) finished executing node({}), op({}), cost({})ms",
            session->Id(), node.node_name(), node.op_type(),
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
                .count());
        if (node.op_type() == "Publish") {
          auto results = session->GetPublishResults();
          for (const auto& result : results) {
            pb::Tensor* out_column = response->add_out_columns();
            out_column->CopyFrom(*result);
          }
        } else if (node.op_type() == "DumpFile") {
          auto affected_rows = session->GetAffectedRows();
          response->set_num_rows_affected(affected_rows);
        }
      }
    }

    if (subdag.need_call_barrier_after_jobs()) {
      yacl::link::Barrier(session->GetLink(), session->Id());
    }
  }

  SPDLOG_INFO("session({}) run plan policy succ", session->Id());
  response->mutable_status()->set_code(pb::Code::OK);
  return;
}

void EngineServiceImpl::VerifyPublicKeys(
    const pb::SessionStartParams& start_params) {
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
  // 1. run jobs in plan and add result to response.
  const std::string session_id = request->session_params().session_id();
  try {
    YACL_ENFORCE(session_mgr_->SetSessionState(session_id, SessionState::IDLE,
                                               SessionState::RUNNING));
    RunPlanCore(*request, session, response);
  } catch (const std::exception& e) {
    if (!session_mgr_->SetSessionState(session_id, SessionState::RUNNING,
                                       SessionState::IDLE)) {
      SPDLOG_WARN(
          "RunExecutionPlan set session to idle failed when exception throw");
    }  // else wait WatchSessionTimeoutThread or StopSession to remove
       // session, because it requires the cooperation of all participating
       // engines

    std::string err_msg = fmt::format(
        "RunExecutionPlan run jobs({}) failed, catch std::exception={} ",
        session_id, e.what());
    LOG_ERROR_AND_SET_STATUS(response->mutable_status(),
                             pb::Code::UNKNOWN_ENGINE_ERROR, err_msg);
    response->mutable_out_columns()->Clear();
    ReportErrorToPeers(request->session_params(),
                       pb::Code::UNKNOWN_ENGINE_ERROR, err_msg);
    MergePeerErrors(session->GetPeerErrors(), response->mutable_status());
    return;
  }

  auto peer_errors = session->GetPeerErrors();
  // 2. remove session.
  try {
    YACL_ENFORCE(session_mgr_->SetSessionState(
        session_id, SessionState::RUNNING, SessionState::IDLE));
    session_mgr_->RemoveSession(session_id);
  } catch (const std::exception& e) {
    std::string err_msg = fmt::format(
        "RunExecutionPlan remove session({}) failed, catch "
        "std::exception={} ",
        session_id, e.what());
    LOG_ERROR_AND_SET_STATUS(response->mutable_status(),
                             pb::Code::UNKNOWN_ENGINE_ERROR, err_msg);
    response->mutable_out_columns()->Clear();
    ReportErrorToPeers(request->session_params(),
                       pb::Code::UNKNOWN_ENGINE_ERROR, err_msg);
    MergePeerErrors(peer_errors, response->mutable_status());
    return;
  }

  SPDLOG_INFO("RunExecutionPlan success, request info:\n{} ",
              MessageToJsonString(*request));
  MergePeerErrors(peer_errors, response->mutable_status());
  return;
}

void EngineServiceImpl::RunPlanAsync(const pb::RunExecutionPlanRequest request,
                                     Session* session,
                                     const std::string& source_ip) {
  auto start_time = std::chrono::system_clock::now();
  pb::RunExecutionPlanResponse response;
  response.set_session_id(request.session_params().session_id());
  response.set_party_code(request.session_params().party_code());

  RunPlanSync(&request, session, &response);
  audit::RecordRunExecPlanEvent(request, response, start_time, source_ip);

  // prepare report info
  pb::ReportRequest report;
  report.mutable_status()->CopyFrom(response.status());
  report.mutable_out_columns()->CopyFrom(response.out_columns());
  report.set_session_id(response.session_id());
  report.set_party_code(response.party_code());
  report.set_num_rows_affected(response.num_rows_affected());

  ReportResult(request.session_params().session_id(), request.callback_url(),
               MessageToJsonString(report));
}

void EngineServiceImpl::ReportErrorToPeers(const pb::SessionStartParams& params,
                                           const pb::Code err_code,
                                           const std::string& err_msg) {
  try {
    PartyInfo parties(params);
    for (const auto& party : parties.AllParties()) {
      if (party.id == parties.SelfPartyCode()) {
        continue;
      }

      auto rpc_channel =
          channel_manager_->Create(party.host, RemoteRole::PeerEngine);
      YACL_ENFORCE(rpc_channel, "create rpc channel failed for party=({},{})",
                   party.id, party.host);

      services::pb::ReportErrorRequest request;
      services::pb::ReportErrorResponse response;
      brpc::Controller cntl;
      services::pb::ErrorCollectorService_Stub stub(rpc_channel.get());

      request.set_session_id(params.session_id());
      request.set_party_code(parties.SelfPartyCode());
      request.mutable_status()->set_code(err_code);
      request.mutable_status()->set_message(err_msg);

      stub.ReportError(&cntl, &request, &response, NULL);
      if (cntl.Failed()) {
        SPDLOG_WARN(
            "sync error to peer=({},{}) rpc failed: error_code: {}, "
            "error_info: {}",
            party.id, party.host, cntl.ErrorCode(), cntl.ErrorText());
      } else {
        if (response.status().code() != pb::Code::OK) {
          SPDLOG_WARN("sync error to peer=({},{}) failed: status: {}", party.id,
                      party.host, response.status().DebugString());
        }
      }
    }
  } catch (const std::exception& e) {
    SPDLOG_WARN("sync error to peers failed: throw: {}", e.what());
  }
}

}  // namespace scql::engine