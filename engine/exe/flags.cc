// Copyright 2024 Ant Group Co., Ltd.
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
//

#include "gflags/gflags.h"

// Log flags
DEFINE_string(
    log_level, "info",
    "log level, can be trace, debug, info, warning, error, critical, off");
DEFINE_string(log_dir, "logs", "log directory");
DEFINE_bool(log_enable_console_logger, true,
            "whether logging to stdout while logging to file");
DEFINE_bool(log_enable_session_logger_separation, false,
            "whether output session-related logs to a dedicated file");

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
DEFINE_bool(enable_separate_link_port, false,
            "whether use a separate port for link service");
DEFINE_int32(link_port, 8004, "port for link service");
DEFINE_int32(idle_timeout_s, 30, "connections idle close delay in seconds");
DEFINE_bool(server_enable_ssl, true,
            "whether brpc server's ssl enable/disable");
DEFINE_bool(server_enable_ssl_client_verification, false,
            "enable ssl client verification");
DEFINE_string(server_ssl_certificate, "",
              "Certificate file path to enable SSL");
DEFINE_string(server_ssl_private_key, "",
              "Private key file path to enable SSL");
DEFINE_string(server_ssl_client_ca_certificate, "",
              "the trusted CA file to verity the client's certificate");
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
DEFINE_string(spu_allowed_protocols, "SEMI2K,ABY3,CHEETAH",
              "spu allowed protocols");
// DataSource connection flags.
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