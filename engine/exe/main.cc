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
#include "engine/datasource/datasource_adaptor_mgr.h"
#include "engine/datasource/embed_router.h"
#include "engine/exe/version.h"
#include "engine/framework/session.h"
#include "engine/link/mux_link_factory.h"
#include "engine/link/mux_receiver_service.h"
#include "engine/link/rpc_helper.h"
#include "engine/services/engine_service_impl.h"
#include "engine/util/logging.h"

// Log flags
DEFINE_string(log_dir, "logs", "log directory");
DEFINE_bool(log_enable_console_logger, true,
            "whether logging to stdout while logging to file");

DEFINE_string(audit_log_dir, "audit", "audit log directory");
DEFINE_bool(enable_audit_logger, true, "whether enable audit log");

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
// Brpc channel flags for Scdb
DEFINE_string(scdb_protocol, "http:proto", "rpc protocol");
DEFINE_string(scdb_connection_type, "pooled", "connection type");
DEFINE_int32(scdb_timeout_ms, 5000, "rpc timeout in milliseconds.");
DEFINE_int32(scdb_max_retry, 3, "rpc max retries(not including the first rpc)");
DEFINE_bool(scdb_enable_ssl_as_client, true, "enable ssl encryption as client");
DEFINE_bool(scdb_enable_ssl_client_verification, false,
            "enable ssl client verification");
DEFINE_string(scdb_ssl_client_ca_certificate, "",
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
DEFINE_bool(enable_scdb_authorization, false,
            "if set to true, the engine will verify the HTTP header "
            "'credential' field of requests from SCDB");
DEFINE_string(engine_credential, "", "scdb authorization credential");
// Session flags
DEFINE_int32(session_timeout_s, 1800,
             "TTL for session, should be greater than the typical runtime of "
             "the specific tasks.");
// DataBase connection flags.
DEFINE_string(datasource_router, "embed", "datasource router type");
DEFINE_string(
    embed_router_conf, "",
    R"text(configuration for embed router in json format. For example: --embed_router_conf={"datasources":[{"id":"ds001","name":"mysql db","kind":"MYSQL","connection_str":"host=127.0.0.1 db=test user=root password='qwerty'"}],"rules":[{"db":"*","table":"*","datasource_id":"ds001"}]} )text");
DEFINE_string(db_connection_info, "",
              "connection string used to connect to mysql...");

void AddChannelOptions(scql::engine::ChannelManager* channel_manager) {
  // PeerEngine Options
  {
    brpc::ChannelOptions options;
    options.protocol = FLAGS_peer_engine_protocol;
    options.connection_type = FLAGS_peer_engine_connection_type;
    options.timeout_ms = FLAGS_peer_engine_timeout_ms;
    options.max_retry = FLAGS_peer_engine_max_retry;

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

    static scql::engine::LogicalRetryPolicy g_my_retry_policy;
    options.retry_policy = &g_my_retry_policy;

    if (FLAGS_enable_client_authorization) {
      options.auth = scql::engine::DefaultAuthenticator();
    }

    channel_manager->AddChannelOptions(scql::engine::RemoteRole::PeerEngine,
                                       options);
  }

  // Scdb Options
  {
    brpc::ChannelOptions options;
    options.protocol = FLAGS_scdb_protocol;
    options.connection_type = FLAGS_scdb_connection_type;
    options.timeout_ms = FLAGS_scdb_timeout_ms;
    options.max_retry = FLAGS_scdb_max_retry;

    if (FLAGS_scdb_enable_ssl_as_client) {
      options.mutable_ssl_options()->ciphers = "";
      if (FLAGS_scdb_enable_ssl_client_verification) {
        options.mutable_ssl_options()->verify.verify_depth = 1;
        options.mutable_ssl_options()->verify.ca_file_path =
            FLAGS_scdb_ssl_client_ca_certificate;
      }
    }

    channel_manager->AddChannelOptions(scql::engine::RemoteRole::Scdb, options);
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
  } else {
    SPDLOG_ERROR("Fail to build router, unsupported datasource router type={}",
                 FLAGS_datasource_router);
    return nullptr;
  }
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
  auto session_manager = std::make_unique<scql::engine::SessionManager>(
      session_opt, listener_manager, std::move(link_factory),
      std::move(ds_router), std::move(ds_mgr), FLAGS_session_timeout_s);

  scql::engine::EngineServiceOptions engine_service_opt;
  engine_service_opt.enable_authorization = FLAGS_enable_scdb_authorization;
  engine_service_opt.credential = FLAGS_engine_credential;
  return std::make_unique<scql::engine::EngineServiceImpl>(
      engine_service_opt, std::move(session_manager), channel_manager);
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

    scql::engine::audit::AuditOptions auditOpts;
    auditOpts.audit_log_dir = FLAGS_audit_log_dir;
    if (FLAGS_enable_audit_logger) {
      auditOpts.enable_audit_logger = true;
    }
    scql::engine::audit::SetupAudit(auditOpts);

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
