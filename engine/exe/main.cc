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
#include "brpc/server.h"
#include "butil/file_util.h"
#include "gflags/gflags.h"

#include "engine/audit/audit_log.h"
#include "engine/auth/authenticator.h"
#include "engine/datasource/datasource_adaptor_mgr.h"
#include "engine/datasource/embed_router.h"
#include "engine/datasource/http_router.h"
#include "engine/datasource/kuscia_datamesh_router.h"
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

// Log flags
DEFINE_string(
    log_level, "info",
    "log level, can be trace, debug, info, warning, error, critical, off");
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
DEFINE_string(peer_engine_load_balancer, "", "load balancer: \"rr\" or empty");
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
DEFINE_int32(http_max_payload_size, 1024 * 1024,
             "max payload to decide whether to send value chunked, default 1M");
// Brpc channel flags for driver(SCDB, SCQLBroker...)
DEFINE_string(driver_protocol, "http:proto", "rpc protocol");
DEFINE_string(driver_connection_type, "pooled", "connection type");
DEFINE_string(driver_load_balancer, "", "load balancer: \"rr\" or empty");
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
DEFINE_string(spu_allowed_protocols, "SEMI2K,ABY3,CHEETAH,SECURENN",
              "spu allowed protocols");
// DataBase connection flags.
DEFINE_string(datasource_router, "embed",
              "datasource router type: embed | http | kusciadatamesh");
DEFINE_string(
    embed_router_conf, "",
    R"text(configuration for embed router in json format. For example: --embed_router_conf={"datasources":[{"id":"ds001","name":"mysql db","kind":"MYSQL","connection_str":"host=127.0.0.1 db=test user=root password='qwerty'"}],"rules":[{"db":"*","table":"*","datasource_id":"ds001"}]} )text");
DEFINE_string(http_router_endpoint, "", "http datasource router endpoint url");
DEFINE_string(kuscia_datamesh_endpoint, "datamesh:8071",
              "kuscia datamesh grpc endpoint");
DEFINE_string(kuscia_datamesh_client_key_path, "",
              "kuscia datamesh client key file");
DEFINE_string(kuscia_datamesh_client_cert_path, "",
              "kuscia datamesh client cert file");
DEFINE_string(kuscia_datamesh_cacert_path, "",
              "kuscia datamesh server cacert file");
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
DEFINE_bool(enable_psi_detail_logger, false, "whether enable detail log");
DEFINE_string(psi_detail_logger_dir, "logs/detail", "log dir");

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
  session_opt.link_recv_timeout_ms = FLAGS_link_recv_timeout_ms;
  if (FLAGS_link_throttle_window_size > 0) {
    session_opt.link_throttle_window_size = FLAGS_link_throttle_window_size;
  }
  if (FLAGS_link_chunked_send_parallel_size > 0) {
    session_opt.link_chunked_send_parallel_size =
        FLAGS_link_chunked_send_parallel_size;
  }
  if (FLAGS_http_max_payload_size > 0) {
    session_opt.http_max_payload_size = FLAGS_http_max_payload_size;
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
  scql::engine::util::LogOptions opts;
  if (FLAGS_enable_psi_detail_logger) {
    opts.enable_psi_detail_logger = true;
    opts.log_dir = FLAGS_psi_detail_logger_dir;
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

  scql::engine::MetricsService metrics_exposer;
  metrics_exposer.RegisterCollectable(
      scql::engine::util::PrometheusMonitor::GetInstance()->GetRegistry());
  SPDLOG_INFO("Adding MetricsService into brpc server");
  if (server.AddService(&metrics_exposer, brpc::SERVER_DOESNT_OWN_SERVICE) !=
      0) {
    SPDLOG_ERROR("Fail to add MetricsService to server");
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