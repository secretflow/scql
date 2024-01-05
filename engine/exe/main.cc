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

#include "absl/debugging/failure_signal_handler.h"
#include "absl/debugging/symbolize.h"
#include "brpc/server.h"
#include "gflags/gflags.h"

#include "engine/audit/audit_log.h"
#include "engine/auth/authenticator.h"
#include "engine/datasource/datasource_adaptor_mgr.h"
#include "engine/datasource/embed_router.h"
#include "engine/datasource/http_router.h"
#include "engine/exe/version.h"
#include "engine/framework/session.h"
#include "engine/link/mux_link_factory.h"
#include "engine/link/mux_receiver_service.h"
#include "engine/link/rpc_helper.h"
#include "engine/services/engine_service_impl.h"
#include "engine/services/error_collector_service_impl.h"
#include "engine/util/logging.h"

// Log flags
DEFINE_string(log_dir, "logs", "log directory");
DEFINE_bool(log_enable_console_logger, true,
            "whether logging to stdout while logging to file");

DEFINE_bool(enable_audit_logger, true, "whether enable audit log");
DEFINE_string(audit_log_file, "audit/audit.log", "audit basic log filename");
DEFINE_string(audit_detail_file, "audit/detail.log",
              "audit detail log filename");
DEFINE_int32(audit_max_files, 180,
             "maximum number of old audit log files to retain");

// Brpc channel flags for peer engine.
DEFINE_string(peer_engine_protocol, "baidu_std", "rpc protocol");
DEFINE_string(peer_engine_connection_type, "single", "connection type");
DEFINE_int32(peer_engine_timeout_ms, 300000, "rpc timeout in milliseconds.");
DEFINE_int32(peer_engine_max_retry, 3,
             "rpc max retries(not including the first rpc)");
DEFINE_bool(peer_engine_enable_ssl_as_client, true,
            "enable ssl encryption as client");
DEFINE_bool(peer_engine_enable_ssl_client_verification, false,
            "enable ssl client verification");
DEFINE_string(peer_engine_ssl_client_ca_certificate, "",
              "certificate Authority file path to verify SSL as client");
DEFINE_int32(link_recv_timeout_ms, 30 * 1000,
             "the max time that a party will wait for a given event");
DEFINE_int32(
    link_throttle_window_size, 16,
    "throttle window size for channel, set to limit the number of messages "
    "sent asynchronously to avoid network congestion, set 0 to disable");
// TODO(zhihe): for rpc stabilization in 100M bandwidth, the default
// link_chunked_send_parallel_size is 1, should find a tradeoff value between
// bandwidth and send speed
DEFINE_int32(link_chunked_send_parallel_size, 1,
             "parallel size when send chunked value");
// Brpc channel flags for driver(SCDB, SCQLBroker...)
DEFINE_string(driver_protocol, "http:proto", "rpc protocol");
DEFINE_string(driver_connection_type, "pooled", "connection type");
DEFINE_int32(driver_timeout_ms, 5000, "rpc timeout in milliseconds.");
DEFINE_int32(driver_max_retry, 3,
             "rpc max retries(not including the first rpc)");
DEFINE_bool(driver_enable_ssl_as_client, true,
            "enable ssl encryption as client");
DEFINE_bool(driver_enable_ssl_client_verification, false,
            "enable ssl client verification");
DEFINE_string(driver_ssl_client_ca_certificate, "",
              "certificate Authority file path to verify SSL as client");
// Brpc server flags.
DEFINE_int32(listen_port, 8003, "");
DEFINE_bool(enable_builtin_service, false,
            "whether brpc builtin service is enable/disable");
DEFINE_int32(internal_port, 9527, "which port the builtin services server on");
DEFINE_int32(idle_timeout_s, 30, "connections idle close delay in seconds");
DEFINE_bool(server_enable_ssl, true,
            "whether brpc server's ssl enable/disable");
DEFINE_string(server_ssl_certificate, "",
              "Certificate file path to enable SSL");
DEFINE_string(server_ssl_private_key, "",
              "Private key file path to enable SSL");
// Common flags for Brpc server and channel of peer engine.
DEFINE_bool(enable_client_authorization, false,
            "if set true, server will check all requests' http header.");
DEFINE_string(auth_credential, "", "authorization credential");
DEFINE_bool(enable_driver_authorization, false,
            "if set to true, the engine will verify the HTTP header "
            "'credential' field of requests from driver");
DEFINE_string(engine_credential, "", "driver authorization credential");
// Session flags
DEFINE_int32(session_timeout_s, 1800,
             "TTL for session, should be greater than the typical runtime of "
             "the specific tasks.");
// DataBase connection flags.
DEFINE_string(datasource_router, "embed",
              "datasource router type: embed | http");
DEFINE_string(
    embed_router_conf, "",
    R"text(configuration for embed router in json format. For example: --embed_router_conf={"datasources":[{"id":"ds001","name":"mysql db","kind":"MYSQL","connection_str":"host=127.0.0.1 db=test user=root password='qwerty'"}],"rules":[{"db":"*","table":"*","datasource_id":"ds001"}]} )text");
DEFINE_string(http_router_endpoint, "", "http datasource router endpoint url");
DEFINE_string(db_connection_info, "",
              "connection string used to connect to mysql...");
// Party authentication flags
DEFINE_bool(enable_self_auth, true,
            "whether enable self identity authentication");
DEFINE_string(private_key_pem_path, "", "path to private key pem file");
DEFINE_bool(enable_peer_auth, true,
            "whether enable peer parties identity authentication");
DEFINE_string(authorized_profile_path, "",
              "path to authorized profile, in json format");

void AddChannelOptions(scql::engine::ChannelManager* channel_manager) {
  // PeerEngine Options
  {
    brpc::ChannelOptions options;
    options.protocol = FLAGS_peer_engine_protocol;
    options.connection_type = FLAGS_peer_engine_connection_type;
    options.timeout_ms = FLAGS_peer_engine_timeout_ms;

    if (FLAGS_peer_engine_enable_ssl_as_client) {
      options.mutable_ssl_options()->ciphers = "";
      if (FLAGS_peer_engine_enable_ssl_client_verification) {
        if (FLAGS_peer_engine_ssl_client_ca_certificate.empty()) {
          SPDLOG_WARN("peer_engine_ssl_client_ca_certificate is empty");
        }
        // All certificate are directly signed by our CA, depth = 1 is enough.
        options.mutable_ssl_options()->verify.verify_depth = 1;
        options.mutable_ssl_options()->verify.ca_file_path =
            FLAGS_peer_engine_ssl_client_ca_certificate;
      }
    }

    if (FLAGS_enable_client_authorization) {
      options.auth = scql::engine::DefaultAuthenticator();
    }

    channel_manager->AddChannelOptions(scql::engine::RemoteRole::PeerEngine,
                                       options);
  }

  // Driver Options
  {
    brpc::ChannelOptions options;
    options.protocol = FLAGS_driver_protocol;
    options.connection_type = FLAGS_driver_connection_type;
    options.timeout_ms = FLAGS_driver_timeout_ms;
    options.max_retry = FLAGS_driver_max_retry;

    if (FLAGS_driver_enable_ssl_as_client) {
      options.mutable_ssl_options()->ciphers = "";
      if (FLAGS_driver_enable_ssl_client_verification) {
        options.mutable_ssl_options()->verify.verify_depth = 1;
        options.mutable_ssl_options()->verify.ca_file_path =
            FLAGS_driver_ssl_client_ca_certificate;
      }
    }

    channel_manager->AddChannelOptions(scql::engine::RemoteRole::Driver,
                                       options);
  }
}

std::unique_ptr<scql::engine::Router> BuildRouter() {
  if (FLAGS_datasource_router == "embed") {
    if (!FLAGS_embed_router_conf.empty()) {
      SPDLOG_INFO("Building EmbedRouter from json conf");
      return scql::engine::EmbedRouter::FromJsonStr(FLAGS_embed_router_conf);
    }

    std::string db_connection_str = FLAGS_db_connection_info;
    if (char* env_p = std::getenv("DB_CONNECTION_INFO")) {
      if (strlen(env_p) != 0) {
        db_connection_str = env_p;
        SPDLOG_INFO("Getting DB_CONNECTION_INFO from ENV");
      }
    }

    if (!db_connection_str.empty()) {
      SPDLOG_INFO("Building EmbedRouter from connection string");
      return scql::engine::EmbedRouter::FromConnectionStr(db_connection_str);
    }

    SPDLOG_ERROR(
        "Fail to build embed router, set "
        "embed_router_conf/db_connection_info(gflags) or "
        "DB_CONNECTION_INFO(ENV)");
    return nullptr;
  } else if (FLAGS_datasource_router == "http") {
    scql::engine::HttpRouterOptions options;
    options.endpoint = FLAGS_http_router_endpoint;
    return std::make_unique<scql::engine::HttpRouter>(options);
  } else {
    SPDLOG_ERROR("Fail to build router, unsupported datasource router type={}",
                 FLAGS_datasource_router);
    return nullptr;
  }
}

std::unique_ptr<scql::engine::auth::Authenticator> BuildAuthenticator() {
  scql::engine::auth::AuthOption option;
  option.enable_self_auth = FLAGS_enable_self_auth;
  option.enable_peer_auth = FLAGS_enable_peer_auth;
  option.private_key_pem_path = FLAGS_private_key_pem_path;
  option.authorized_profile_path = FLAGS_authorized_profile_path;

  return std::make_unique<scql::engine::auth::Authenticator>(option);
}

std::unique_ptr<scql::engine::EngineServiceImpl> BuildEngineService(
    scql::engine::ListenerManager* listener_manager,
    scql::engine::ChannelManager* channel_manager) {
  auto link_factory = std::make_unique<scql::engine::MuxLinkFactory>(
      channel_manager, listener_manager);

  std::unique_ptr<scql::engine::Router> ds_router = BuildRouter();
  YACL_ENFORCE(ds_router);

  std::unique_ptr<scql::engine::DatasourceAdaptorMgr> ds_mgr =
      std::make_unique<scql::engine::DatasourceAdaptorMgr>();

  scql::engine::SessionOptions session_opt;
  session_opt.link_recv_timeout_ms = FLAGS_link_recv_timeout_ms;
  if (FLAGS_link_throttle_window_size > 0) {
    session_opt.link_throttle_window_size = FLAGS_link_throttle_window_size;
  }
  if (FLAGS_link_chunked_send_parallel_size > 0) {
    session_opt.link_chunked_send_parallel_size =
        FLAGS_link_chunked_send_parallel_size;
  }
  // NOTE: use yacl retry options to replace brpc retry policy
  yacl::link::RetryOptions retry_opt;
  retry_opt.max_retry = FLAGS_peer_engine_max_retry;
  retry_opt.http_codes = {
      brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR,
      brpc::HTTP_STATUS_GATEWAY_TIMEOUT, brpc::HTTP_STATUS_BAD_GATEWAY,
      brpc::HTTP_STATUS_REQUEST_TIMEOUT, brpc::HTTP_STATUS_SERVICE_UNAVAILABLE,
      // too many connections
      429};
  retry_opt.error_codes = {brpc::Errno::EFAILEDSOCKET,
                           brpc::Errno::EEOF,
                           brpc::Errno::ELOGOFF,
                           brpc::Errno::ELIMIT,
                           ETIMEDOUT,
                           ENOENT,
                           EPIPE,
                           ECONNREFUSED,
                           ECONNRESET,
                           ENODATA,
                           brpc::Errno::EOVERCROWDED,
                           brpc::Errno::EH2RUNOUTSTREAMS};
  session_opt.link_retry_options = retry_opt;
  auto session_manager = std::make_unique<scql::engine::SessionManager>(
      session_opt, listener_manager, std::move(link_factory),
      std::move(ds_router), std::move(ds_mgr), FLAGS_session_timeout_s);

  auto authenticator = BuildAuthenticator();

  scql::engine::EngineServiceOptions engine_service_opt;
  engine_service_opt.enable_authorization = FLAGS_enable_driver_authorization;
  engine_service_opt.credential = FLAGS_engine_credential;
  return std::make_unique<scql::engine::EngineServiceImpl>(
      engine_service_opt, std::move(session_manager), channel_manager,
      std::move(authenticator));
}

brpc::ServerOptions BuildServerOptions() {
  brpc::ServerOptions options;
  options.has_builtin_services = FLAGS_enable_builtin_service;
  options.internal_port = FLAGS_internal_port;
  options.idle_timeout_sec = FLAGS_idle_timeout_s;
  if (FLAGS_server_enable_ssl) {
    if (FLAGS_server_ssl_certificate.empty() ||
        FLAGS_server_ssl_private_key.empty()) {
      SPDLOG_WARN("server ssl cert or key file is empty");
    }
    auto ssl_options = options.mutable_ssl_options();
    ssl_options->default_cert.certificate = FLAGS_server_ssl_certificate;
    ssl_options->default_cert.private_key = FLAGS_server_ssl_private_key;
    ssl_options->ciphers = "";
  }
  if (FLAGS_enable_client_authorization) {
    options.auth = scql::engine::DefaultAuthenticator();
  }

  return options;
}

int main(int argc, char* argv[]) {
  // Initialize the symbolizer to get a human-readable stack trace
  {
    absl::InitializeSymbolizer(argv[0]);

    absl::FailureSignalHandlerOptions options;
    absl::InstallFailureSignalHandler(options);
  }

  gflags::SetVersionString(ENGINE_VERSION_STRING);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Setup Logger.
  try {
    scql::engine::util::LogOptions opts;
    opts.log_dir = FLAGS_log_dir;
    if (FLAGS_log_enable_console_logger) {
      opts.enable_console_logger = true;
    }
    scql::engine::util::SetupLogger(opts);

    if (FLAGS_enable_audit_logger) {
      scql::engine::audit::AuditOptions auditOpts;
      auditOpts.audit_log_file = FLAGS_audit_log_file;
      auditOpts.audit_detail_file = FLAGS_audit_detail_file;
      auditOpts.audit_max_files = FLAGS_audit_max_files;
      scql::engine::audit::SetupAudit(auditOpts);
    }

  } catch (const std::exception& e) {
    SPDLOG_ERROR("Fail to setup logger, msg={}", e.what());
    return -1;
  }

  // Set default authenticator for brpc channel.
  if (FLAGS_enable_client_authorization) {
    auto auth = std::make_unique<scql::engine::SimpleAuthenticator>(
        FLAGS_auth_credential);
    scql::engine::SetDefaultAuthenticator(std::move(auth));
  }

  // Setup brpc server
  brpc::Server server;
  server.set_version(ENGINE_VERSION_STRING);

  scql::engine::ListenerManager listener_manager;
  scql::engine::MuxReceiverServiceImpl rcv_svc(&listener_manager);
  SPDLOG_INFO("Adding MuxReceiverService into brpc server");
  if (server.AddService(&rcv_svc, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    SPDLOG_ERROR("Fail to add MuxReceiverService to server");
    return -1;
  }

  scql::engine::ChannelManager channel_manager;
  try {
    AddChannelOptions(&channel_manager);
  } catch (const std::exception& e) {
    SPDLOG_ERROR("Fail to add channel options, msg={}", e.what());
    return -1;
  }
  std::unique_ptr<scql::engine::EngineServiceImpl> engine_svc;
  try {
    engine_svc = BuildEngineService(&listener_manager, &channel_manager);
    YACL_ENFORCE(engine_svc);
  } catch (const std::exception& e) {
    SPDLOG_ERROR("Fail to build engine service, msg={}", e.what());
    return -1;
  }
  SPDLOG_INFO("Adding EngineService into brpc server");
  if (server.AddService(engine_svc.get(), brpc::SERVER_DOESNT_OWN_SERVICE) !=
      0) {
    SPDLOG_ERROR("Fail to add EngineService to server");
    return -1;
  }

  scql::engine::ErrorCollectorServiceImpl err_svc(
      engine_svc->GetSessionManager());
  SPDLOG_INFO("Adding ErrorCollectorService into brpc server");
  if (server.AddService(&err_svc, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    SPDLOG_ERROR("Fail to add ErrorCollectorService to server");
    return -1;
  }

  // Start brpc server.
  brpc::ServerOptions options = BuildServerOptions();
  if (server.Start(FLAGS_listen_port, &options) != 0) {
    SPDLOG_ERROR("Fail to start engine rpc server on port={}",
                 FLAGS_listen_port);
    return -1;
  }
  SPDLOG_INFO("Started engine rpc server success, listen on: {}",
              butil::endpoint2str(server.listen_address()).c_str());

  // Wait until Ctrl-C is pressed, then Stop() and Join() the server.
  server.RunUntilAskedToQuit();

  return 0;
}