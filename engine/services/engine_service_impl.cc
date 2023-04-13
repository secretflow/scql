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

#include <utility>

#include "brpc/channel.h"
#include "brpc/closure_guard.h"
#include "google/protobuf/util/json_util.h"

#include "engine/framework/exec.h"
#include "engine/framework/executor.h"
#include "engine/operator/all_ops_register.h"
#include "engine/util/tensor_util.h"

#include "api/status_code.pb.h"

#ifndef LOG_ERROR_AND_SET_RESPONSE
#define LOG_ERROR_AND_SET_RESPONSE(err_code, err_msg) \
  do {                                                \
    SPDLOG_ERROR(err_msg);                            \
    response->mutable_status()->set_code(err_code);   \
    response->mutable_status()->set_message(err_msg); \
  } while (false)
#endif  // LOG_ERROR_AND_SET_RESPONSE

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

}  // namespace

namespace scql::engine {

EngineServiceImpl::EngineServiceImpl(
    const EngineServiceOptions& options,
    std::unique_ptr<SessionManager> session_mgr,
    ChannelManager* channel_manager)
    : service_options_(options),
      session_mgr_(std::move(session_mgr)),
      channel_manager_(channel_manager) {
  if (options.enable_authorization && options.credential.empty()) {
    YACL_THROW(
        "credential is empty, you should provide credential for scdb "
        "authorization");
  }
  op::RegisterAllOps();
}

bool EngineServiceImpl::CheckSCDBCredential(
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
    SPDLOG_ERROR("scdb authorization failed, unknown scdb credential");
    return false;
  }
  return true;
}

void EngineServiceImpl::StartSession(::google::protobuf::RpcController* cntl,
                                     const pb::StartSessionRequest* request,
                                     pb::StartSessionResponse* response,
                                     ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  // TODO(jingshi): Add opentelemetry.
  // check illegal request.
  auto controller = static_cast<brpc::Controller*>(cntl);
  if (!CheckSCDBCredential(controller->http_request())) {
    std::string err_msg = "scdb authentication failed";
    LOG_ERROR_AND_SET_RESPONSE(pb::Code::UNAUTHENTICATED, err_msg);
    return;
  }
  if (request->session_params().session_id().empty()) {
    std::string err_msg = "session_id in request is empty";
    LOG_ERROR_AND_SET_RESPONSE(pb::Code::INVALID_ARGUMENT, err_msg);
    return;
  }

  SPDLOG_INFO("EngineServiceImpl::StartSession with session id: {}",
              request->session_params().session_id());
  try {
    session_mgr_->CreateSession(request->session_params());

    response->mutable_status()->set_code(pb::Code::OK);
    response->mutable_status()->set_message("ok");
    return;
  } catch (const std::exception& e) {
    std::string err_msg = fmt::format(
        "StartSession for session_id={} failed, catch std::exception={} ",
        request->session_params().session_id(), e.what());
    LOG_ERROR_AND_SET_RESPONSE(pb::Code::UNKNOWN_ENGINE_ERROR, err_msg);
    return;
  }
}

void EngineServiceImpl::RunDag(::google::protobuf::RpcController* cntl,
                               const pb::RunDagRequest* request,
                               pb::RunDagResponse* response,
                               ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  // check illegal request.
  auto controller = static_cast<brpc::Controller*>(cntl);
  if (!CheckSCDBCredential(controller->http_request())) {
    std::string err_msg = "scdb authentication failed";
    LOG_ERROR_AND_SET_RESPONSE(pb::Code::UNAUTHENTICATED, err_msg);
    return;
  }
  if (request->session_id().empty()) {
    std::string err_msg = "session_id in request is empty";
    LOG_ERROR_AND_SET_RESPONSE(pb::Code::INVALID_ARGUMENT, err_msg);
    return;
  }
  if (request->callback_uri().empty() || request->callback_host().empty()) {
    std::string err_msg = "callback uri/host cannot be null";
    LOG_ERROR_AND_SET_RESPONSE(pb::Code::INVALID_ARGUMENT, err_msg);
    return;
  }
  auto session = session_mgr_->GetSession(request->session_id());
  if (session == nullptr) {
    std::string err_msg = fmt::format("no exist session for session_id({})",
                                      request->session_id());
    LOG_ERROR_AND_SET_RESPONSE(pb::Code::INVALID_ARGUMENT, err_msg);
    return;
  }

  auto ret = session_mgr_->SetSessionState(
      request->session_id(), SessionState::IDLE, SessionState::RUNNING);
  if (!ret) {
    std::string err_msg =
        fmt::format("session({}) running before RunDag", request->session_id());
    LOG_ERROR_AND_SET_RESPONSE(pb::Code::UNKNOWN_ENGINE_ERROR, err_msg);
    return;
  }

  // Copy 'request' to avoid deconstruction before async call finished.
  worker_pool_.Submit(&EngineServiceImpl::RunDagWithSession, this, *request,
                      session);
  SPDLOG_INFO("submit rundag for session({}), dag queue length={}",
              request->session_id(), worker_pool_.GetQueueLength());

  response->mutable_status()->set_code(pb::Code::OK);
  response->mutable_status()->set_message("ok");
  return;
}

void EngineServiceImpl::StopSession(::google::protobuf::RpcController* cntl,
                                    const pb::StopSessionRequest* request,
                                    pb::StopSessionResponse* response,
                                    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  // check illegal request.
  auto controller = static_cast<brpc::Controller*>(cntl);
  if (!CheckSCDBCredential(controller->http_request())) {
    std::string err_msg = "scdb authentication failed";
    LOG_ERROR_AND_SET_RESPONSE(pb::Code::UNAUTHENTICATED, err_msg);
    return;
  }
  if (request->session_id().empty()) {
    std::string err_msg = "session_id in request is empty";
    LOG_ERROR_AND_SET_RESPONSE(pb::Code::INVALID_ARGUMENT, err_msg);
    return;
  }
  SPDLOG_INFO("EngineServiceImpl::StopSession({}), reason({})",
              request->session_id(), request->reason());
  try {
    session_mgr_->RemoveSession(request->session_id());

    response->mutable_status()->set_code(pb::Code::OK);
    response->mutable_status()->set_message("ok");
    return;
  } catch (const std::exception& e) {
    std::string err_msg =
        fmt::format("StopSession({}) failed, catch std::exception={} ",
                    request->session_id(), e.what());
    SPDLOG_WARN(err_msg);

    response->mutable_status()->set_code(pb::Code::UNKNOWN_ENGINE_ERROR);
    response->mutable_status()->set_message(err_msg);
    return;
  }
}

void EngineServiceImpl::RunExecutionPlan(
    ::google::protobuf::RpcController* cntl,
    const pb::RunExecutionPlanRequest* request,
    pb::RunExecutionPlanResponse* response, ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);

  auto controller = static_cast<brpc::Controller*>(cntl);
  if (!CheckSCDBCredential(controller->http_request())) {
    std::string err_msg = "scdb authentication failed";
    LOG_ERROR_AND_SET_RESPONSE(pb::Code::UNAUTHENTICATED, err_msg);
    return;
  }

  const std::string session_id = request->session_params().session_id();
  response->set_session_id(session_id);
  response->set_party_code(request->session_params().party_code());

  Session* session = nullptr;
  // 1. create session first.
  try {
    session_mgr_->CreateSession(request->session_params());
    session = session_mgr_->GetSession(session_id);
    YACL_ENFORCE(session, "get session failed");
  } catch (const std::exception& e) {
    std::string err_msg = fmt::format(
        "RunExecutionPlan create session({}) failed, catch "
        "std::exception={} ",
        session_id, e.what());
    LOG_ERROR_AND_SET_RESPONSE(pb::Code::UNKNOWN_ENGINE_ERROR, err_msg);
    return;
  }

  // 2. run jobs in plan and add result to response.
  try {
    YACL_ENFORCE(session_mgr_->SetSessionState(session_id, SessionState::IDLE,
                                               SessionState::RUNNING));
    RunPlan(*request, session, response);
    YACL_ENFORCE(session_mgr_->SetSessionState(
        session_id, SessionState::RUNNING, SessionState::IDLE));
  } catch (const std::exception& e) {
    std::string err_msg = fmt::format(
        "RunExecutionPlan run jobs({}) failed, catch std::exception={} ",
        session_id, e.what());
    LOG_ERROR_AND_SET_RESPONSE(pb::Code::UNKNOWN_ENGINE_ERROR, err_msg);
    response->mutable_out_columns()->Clear();
    return;
  }

  // 3. remove session.
  try {
    session_mgr_->RemoveSession(session_id);
  } catch (const std::exception& e) {
    std::string err_msg = fmt::format(
        "RunExecutionPlan remove session({}) failed, catch "
        "std::exception={} ",
        session_id, e.what());
    LOG_ERROR_AND_SET_RESPONSE(pb::Code::UNKNOWN_ENGINE_ERROR, err_msg);
    response->mutable_out_columns()->Clear();
    return;
  }

  SPDLOG_INFO("RunExecutionPlan success, request info:\n{} ",
              MessageToJsonString(*request));
  return;
}

void EngineServiceImpl::RunDagWithSession(const pb::RunDagRequest request,
                                          Session* session) {
  pb::Status status;
  try {
    // TODO(jingshi): support async run SubDag's nodes.
    for (int idx = 0; idx < request.nodes_size(); ++idx) {
      const auto& node = request.nodes(idx);

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
    }

    status.set_code(pb::Code::OK);
  } catch (const std::exception& e) {
    std::string err_msg =
        fmt::format("RunDag for session_id={} failed, catch std::exception={} ",
                    request.session_id(), e.what());
    SPDLOG_WARN(err_msg);

    status.set_code(pb::Code::UNKNOWN_ENGINE_ERROR);
    status.set_message(err_msg);
  }

  if (!session_mgr_->SetSessionState(
          request.session_id(), SessionState::RUNNING, SessionState::IDLE)) {
    SPDLOG_WARN("set session({}) state failed after running");
  }

  std::string report_info_str = ConstructReportInfo(status, request, session);
  ReportToScdb(request, report_info_str);
  return;
}

std::string EngineServiceImpl::ConstructReportInfo(
    const pb::Status& status, const pb::RunDagRequest& request,
    Session* session) {
  pb::ReportRequest report;
  report.mutable_status()->CopyFrom(status);
  report.set_dag_id(request.dag_id());
  report.set_session_id(request.session_id());
  report.set_party_code(session->SelfPartyCode());

  for (int i = 0; i < request.nodes_size(); i++) {
    const auto& node = request.nodes(i);
    if (node.op_type() == "Publish") {
      auto results = session->GetPublishResults();
      for (const auto& result : results) {
        pb::Tensor* out_column = report.add_out_columns();
        out_column->CopyFrom(*result);
      }
    } else if (node.op_type() == "DumpFile") {
      auto affected_rows = session->GetAffectedRows();
      report.set_num_rows_affected(affected_rows);
    }
  }

  return MessageToJsonString(report);
}

void EngineServiceImpl::ReportToScdb(const pb::RunDagRequest& request,
                                     const std::string& report_info_str) {
  try {
    auto rpc_channel =
        channel_manager_->Create(request.callback_host(), RemoteRole::Scdb);
    if (!rpc_channel) {
      SPDLOG_WARN(
          "create rpc channel failed for scdb: session_id={}, "
          "callback_host={}",
          request.session_id(), request.callback_host());
      return;
    }

    // do http report
    brpc::Controller cntl;
    cntl.http_request().uri().set_path(request.callback_uri());
    cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
    cntl.http_request().set_content_type("application/json");
    cntl.request_attachment().append(report_info_str);

    SPDLOG_INFO("session({}) send rpc request to scdb, callback_host={}",
                request.session_id(), request.callback_host());
    // Because `done'(last parameter) is NULL, this function waits until
    // the response comes back or error occurs(including timeout).
    rpc_channel->CallMethod(nullptr, &cntl, nullptr, nullptr, nullptr);
    if (cntl.Failed()) {
      SPDLOG_WARN("session({}) report host({}) failed with error({})",
                  request.session_id(), request.callback_host(),
                  cntl.ErrorText());
      return;
    }

    SPDLOG_INFO("session({}) report success, get response ({}), used({}ms)",
                request.session_id(), cntl.response_attachment().to_string(),
                cntl.latency_us() / 1000);

  } catch (const std::exception& e) {
    SPDLOG_WARN("ReportToScdb({}) failed, catch std::exception={} ",
                request.session_id(), e.what());
    return;
  }

  return;
}

void EngineServiceImpl::RunPlan(const pb::RunExecutionPlanRequest& request,
                                Session* session,
                                pb::RunExecutionPlanResponse* response) {
  const auto& policy = request.policy();
  for (const auto& subdag : policy.subdags()) {
    for (const auto& job : subdag.jobs()) {
      const auto& node_ids = job.node_ids();
      for (const auto& node_id : node_ids) {
        const auto& iter = request.nodes().find(node_id);
        YACL_ENFORCE(iter != request.nodes().cend(),
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
  response->mutable_status()->set_message("ok");
  return;
}

}  // namespace scql::engine
