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

#include <memory>

#include "absl/debugging/failure_signal_handler.h"
#include "absl/debugging/symbolize.h"
#include "absl/strings/str_split.h"
#include "absl/strings/strip.h"
#include "arrow/filesystem/s3fs.h"
#include "brpc/server.h"
#include "butil/file_util.h"
#include "flags.h"

#include "engine/audit/audit_log.h"
#include "engine/auth/authenticator.h"
#include "engine/datasource/datasource_adaptor_mgr.h"
#include "engine/datasource/embed_router.h"
#include "engine/datasource/http_router.h"
#include "engine/datasource/kuscia_datamesh_router.h"
#include "engine/exe/flags.h"
#include "engine/exe/version.h"
#include "engine/framework/session.h"
#include "engine/link/mux_link_factory.h"
#include "engine/link/mux_receiver_service.h"
#include "engine/link/rpc_helper.h"
#include "engine/services/engine_service_impl.h"
#include "engine/services/error_collector_service_impl.h"
#include "engine/services/prometheus_service_impl.h"
#include "engine/util/logging.h"
#include "engine/util/prometheus_monitor.h"

grpc::SslCredentialsOptions LoadSslCredentialsOptions(
    const std::string& key_file, const std::string& cert_file,
    const std::string& cacert_file);

void AddChannelOptions(scql::engine::ChannelManager* channel_manager) {
  // PeerEngine Options
  {
    brpc::ChannelOptions brpc_options;
    brpc_options.protocol = FLAGS_peer_engine_protocol;
    brpc_options.connection_type = FLAGS_peer_engine_connection_type;
    brpc_options.timeout_ms = FLAGS_peer_engine_timeout_ms;

    if (FLAGS_peer_engine_enable_ssl_as_client) {
      brpc_options.mutable_ssl_options()->ciphers = "";
      if (FLAGS_peer_engine_enable_ssl_client_verification) {
        if (FLAGS_peer_engine_ssl_client_ca_certificate.empty()) {
          SPDLOG_WARN("peer_engine_ssl_client_ca_certificate is empty");
        }
        // All certificate are directly signed by our CA, depth = 1 is enough.
        brpc_options.mutable_ssl_options()->verify.verify_depth = 1;
        brpc_options.mutable_ssl_options()->verify.ca_file_path =
            FLAGS_peer_engine_ssl_client_ca_certificate;
      }
    }

    if (FLAGS_enable_client_authorization) {
      brpc_options.auth = scql::engine::DefaultAuthenticator();
    }

    scql::engine::ChannelOptions options;
    options.brpc_options = brpc_options;
    options.load_balancer = FLAGS_peer_engine_load_balancer;

    channel_manager->AddChannelOptions(scql::engine::RemoteRole::PeerEngine,
                                       options);
  }

  // Driver Options
  {
    brpc::ChannelOptions brpc_options;
    brpc_options.protocol = FLAGS_driver_protocol;
    brpc_options.connection_type = FLAGS_driver_connection_type;
    brpc_options.timeout_ms = FLAGS_driver_timeout_ms;
    brpc_options.max_retry = FLAGS_driver_max_retry;

    if (FLAGS_driver_enable_ssl_as_client) {
      brpc_options.mutable_ssl_options()->ciphers = "";
      if (FLAGS_driver_enable_ssl_client_verification) {
        brpc_options.mutable_ssl_options()->verify.verify_depth = 1;
        brpc_options.mutable_ssl_options()->verify.ca_file_path =
            FLAGS_driver_ssl_client_ca_certificate;
      }
    }

    scql::engine::ChannelOptions options;
    options.brpc_options = brpc_options;
    options.load_balancer = FLAGS_driver_load_balancer;

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
  } else if (FLAGS_datasource_router == "kusciadatamesh") {
    auto ssl_opts =
        LoadSslCredentialsOptions(FLAGS_kuscia_datamesh_client_key_path,
                                  FLAGS_kuscia_datamesh_client_cert_path,
                                  FLAGS_kuscia_datamesh_cacert_path);
    auto channel_creds = grpc::SslCredentials(ssl_opts);
    return std::make_unique<scql::engine::KusciaDataMeshRouter>(
        FLAGS_kuscia_datamesh_endpoint, channel_creds);

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
  session_opt.link_config.link_recv_timeout_ms = FLAGS_link_recv_timeout_ms;
  if (FLAGS_link_throttle_window_size > 0) {
    session_opt.link_config.link_throttle_window_size =
        FLAGS_link_throttle_window_size;
  }
  if (FLAGS_link_chunked_send_parallel_size > 0) {
    session_opt.link_config.link_chunked_send_parallel_size =
        FLAGS_link_chunked_send_parallel_size;
  }
  if (FLAGS_http_max_payload_size > 0) {
    session_opt.link_config.http_max_payload_size = FLAGS_http_max_payload_size;
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
  session_opt.link_config.link_retry_options = retry_opt;
  scql::engine::util::LogOptions opts;
  if (FLAGS_enable_psi_detail_logger) {
    opts.enable_psi_detail_logger = true;
    opts.log_dir = FLAGS_psi_detail_logger_dir;
  }
  if (FLAGS_log_enable_console_logger) {
    opts.enable_console_logger = true;
  }
  if (FLAGS_log_enable_session_logger_separation) {
    opts.enable_session_logger_separation = true;
  }
  session_opt.log_options = opts;

  std::vector<spu::ProtocolKind> allowed_protocols;

  std::vector<absl::string_view> protocols_str =
      absl::StrSplit(FLAGS_spu_allowed_protocols, ',');
  for (auto& protocol_str : protocols_str) {
    std::string stripped_str(absl::StripAsciiWhitespace(protocol_str));
    spu::ProtocolKind protocol_kind;

    YACL_ENFORCE(spu::ProtocolKind_Parse(stripped_str, &protocol_kind),
                 fmt::format("invalid protocol provided: {}", stripped_str));
    allowed_protocols.push_back(protocol_kind);
  }

  auto session_manager = std::make_unique<scql::engine::SessionManager>(
      session_opt, listener_manager, std::move(link_factory),
      std::move(ds_router), std::move(ds_mgr), FLAGS_session_timeout_s,
      allowed_protocols);

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
    auto* ssl_options = options.mutable_ssl_options();
    ssl_options->default_cert.certificate = FLAGS_server_ssl_certificate;
    ssl_options->default_cert.private_key = FLAGS_server_ssl_private_key;
    ssl_options->ciphers = "";
    if (FLAGS_server_enable_ssl_client_verification) {
      ssl_options->verify.verify_depth = 2;
      ssl_options->verify.ca_file_path = FLAGS_server_ssl_client_ca_certificate;
    }
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
    opts.log_level = FLAGS_log_level;
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

  scql::engine::MetricsService metrics_exposer;
  metrics_exposer.RegisterCollectable(
      scql::engine::util::PrometheusMonitor::GetInstance()->GetRegistry());
  SPDLOG_INFO("Adding MetricsService into brpc server");
  if (server.AddService(&metrics_exposer, brpc::SERVER_DOESNT_OWN_SERVICE) !=
      0) {
    SPDLOG_ERROR("Fail to add MetricsService to server");
    return -1;
  }

  scql::engine::ErrorCollectorServiceImpl err_svc(
      engine_svc->GetSessionManager());

  brpc::Server link_svr;
  scql::engine::MuxReceiverServiceImpl rcv_svc(&listener_manager);
  if (!FLAGS_enable_separate_link_port) {
    SPDLOG_INFO("Adding MuxReceiverService into main server...");
    if (server.AddService(&rcv_svc, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      SPDLOG_ERROR("Fail to add MuxReceiverService to main server");
      return -1;
    }
    SPDLOG_INFO("Adding ErrorCollectorService into main server...");
    if (server.AddService(&err_svc, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      SPDLOG_ERROR("Fail to add ErrorCollectorService to server");
      return -1;
    }
  } else {
    SPDLOG_INFO("Adding MuxReceiverService into seperate link server...");
    if (link_svr.AddService(&rcv_svc, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      SPDLOG_ERROR("Fail to add MuxReceiverService to seperate link server");
      return -1;
    }
    SPDLOG_INFO("Adding ErrorCollectorService into seperate link server...");
    if (link_svr.AddService(&err_svc, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      SPDLOG_ERROR("Fail to add ErrorCollectorService to seperate link server");
      return -1;
    }
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

  if (FLAGS_enable_separate_link_port) {
    if (FLAGS_link_port == FLAGS_listen_port) {
      SPDLOG_ERROR(
          "conf invalid, link_port should not be same as "
          "listen_port if "
          "--enable_separate_link_port=true");
      return -1;
    }

    brpc::ServerOptions link_svr_opts = BuildServerOptions();
    link_svr_opts.has_builtin_services = false;

    if (link_svr.Start(FLAGS_link_port, &link_svr_opts) != 0) {
      SPDLOG_ERROR("Fail to start link server on port={}", FLAGS_link_port);
      return -1;
    }

    SPDLOG_INFO("Started link server success, listen on: {}",
                butil::endpoint2str(link_svr.listen_address()).c_str());
  }

  // Wait until Ctrl-C is pressed, then Stop() and Join() the server.
  server.RunUntilAskedToQuit();

  if (FLAGS_enable_separate_link_port) {
    link_svr.Stop(0);
    link_svr.Join();
  }

  // Release arrow s3 file resources safely.
  // TODO(jingshi): remove to DumpFile operator to reduce coupling
  if (arrow::fs::IsS3Initialized()) {
    auto status = arrow::fs::FinalizeS3();
    if (!status.ok()) {
      SPDLOG_ERROR("Fail to FinalizeS3, msg={}", status.ToString());
    }
  }

  return 0;
}

grpc::SslCredentialsOptions LoadSslCredentialsOptions(
    const std::string& key_file, const std::string& cert_file,
    const std::string& cacert_file) {
  grpc::SslCredentialsOptions opts;

  std::string content;
  YACL_ENFORCE(butil::ReadFileToString(butil::FilePath(cacert_file), &content));
  opts.pem_root_certs = content;

  YACL_ENFORCE(butil::ReadFileToString(butil::FilePath(key_file), &content));
  opts.pem_private_key = content;

  YACL_ENFORCE(butil::ReadFileToString(butil::FilePath(cert_file), &content));
  opts.pem_cert_chain = content;
  return opts;
}