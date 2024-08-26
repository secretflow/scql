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

#pragma once

#include <gflags/gflags_declare.h>

// Log flags
DECLARE_string(log_level);
DECLARE_string(log_dir);
DECLARE_bool(log_enable_console_logger);
DECLARE_bool(log_enable_session_logger_separation);

DECLARE_bool(enable_audit_logger);
DECLARE_string(audit_log_file);
DECLARE_string(audit_detail_file);
DECLARE_int32(audit_max_files);

// Brpc channel flags for peer engine.
DECLARE_string(peer_engine_protocol);
DECLARE_string(peer_engine_connection_type);
DECLARE_string(peer_engine_load_balancer);
DECLARE_int32(peer_engine_timeout_ms);
DECLARE_int32(peer_engine_max_retry);
DECLARE_bool(peer_engine_enable_ssl_as_client);
DECLARE_bool(peer_engine_enable_ssl_client_verification);
DECLARE_string(peer_engine_ssl_client_ca_certificate);
DECLARE_int32(link_recv_timeout_ms);
DECLARE_int32(link_throttle_window_size);
DECLARE_int32(link_chunked_send_parallel_size);
DECLARE_int32(http_max_payload_size);

// Brpc channel flags for driver(SCDB, SCQLBroker...)
DECLARE_string(driver_protocol);
DECLARE_string(driver_connection_type);
DECLARE_string(driver_load_balancer);
DECLARE_int32(driver_timeout_ms);
DECLARE_int32(driver_max_retry);
DECLARE_bool(driver_enable_ssl_as_client);
DECLARE_bool(driver_enable_ssl_client_verification);
DECLARE_string(driver_ssl_client_ca_certificate);

// Brpc server flags.
DECLARE_int32(listen_port);
DECLARE_bool(enable_builtin_service);
DECLARE_int32(internal_port);
DECLARE_bool(enable_separate_link_port);
DECLARE_int32(link_port);
DECLARE_int32(idle_timeout_s);
DECLARE_bool(server_enable_ssl);
DECLARE_bool(server_enable_ssl_client_verification);
DECLARE_string(server_ssl_certificate);
DECLARE_string(server_ssl_private_key);
DECLARE_string(server_ssl_client_ca_certificate);

// Common flags for Brpc server and channel of peer engine.
DECLARE_bool(enable_client_authorization);
DECLARE_string(auth_credential);
DECLARE_bool(enable_driver_authorization);
DECLARE_string(engine_credential);

// Session flags
DECLARE_int32(session_timeout_s);
DECLARE_string(spu_allowed_protocols);

// DataSource connection flags.
DECLARE_string(datasource_router);
DECLARE_string(embed_router_conf);
DECLARE_string(http_router_endpoint);
DECLARE_string(kuscia_datamesh_endpoint);
DECLARE_string(kuscia_datamesh_client_key_path);
DECLARE_string(kuscia_datamesh_client_cert_path);
DECLARE_string(kuscia_datamesh_cacert_path);
DECLARE_string(db_connection_info);

// Party authentication flags
DECLARE_bool(enable_self_auth);
DECLARE_string(private_key_pem_path);
DECLARE_bool(enable_peer_auth);
DECLARE_string(authorized_profile_path);
DECLARE_bool(enable_psi_detail_logger);
DECLARE_string(psi_detail_logger_dir);