// Copyright 2025 Ant Group Co., Ltd.
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

#include "engine/util/kpad_task_helper.h"

#include "google/protobuf/util/json_util.h"
#include "libspu/core/config.h"
#include "spdlog/spdlog.h"

// Include rapidjson headers from brpc butil
#include "butil/third_party/rapidjson/document.h"
#include "butil/third_party/rapidjson/error/en.h"
#include "butil/third_party/rapidjson/rapidjson.h"

namespace scql::engine::util {

bool ValidateKpadTaskParams(const std::string& cluster_def,
                            const std::string& job_id,
                            const std::string& scql_config) {
  bool has_cluster_def = !cluster_def.empty();
  bool has_job_id = !job_id.empty();
  bool has_scql_config = !scql_config.empty();

  if (has_cluster_def != has_job_id || has_job_id != has_scql_config) {
    SPDLOG_ERROR(
        "Invalid kpad task parameters: kpad_cluster_def, kpad_job_id, and "
        "kpad_scql_config must all be provided or all be empty");
    return false;
  }
  return true;
}

std::unique_ptr<ClusterDef> ParseClusterDef(const std::string& json_str) {
  butil::rapidjson::Document doc;
  doc.Parse(json_str.c_str());
  if (doc.HasParseError()) {
    SPDLOG_ERROR("Failed to parse cluster_def JSON: {} at offset {}",
                 butil::rapidjson::GetParseError_En(doc.GetParseError()),
                 doc.GetErrorOffset());
    return nullptr;
  }
  if (!doc.IsObject()) {
    SPDLOG_ERROR("cluster_def JSON must be an object");
    return nullptr;
  }
  auto cluster_def = std::make_unique<ClusterDef>();
  // Parse self_party
  if (doc.HasMember("self_party") && doc["self_party"].IsString()) {
    cluster_def->self_party = doc["self_party"].GetString();
  } else {
    SPDLOG_ERROR("cluster_def missing or invalid 'self_party' field");
    return nullptr;
  }
  // Parse parties
  if (doc.HasMember("parties") && doc["parties"].IsArray()) {
    const auto& parties_array = doc["parties"];
    for (const auto* it = parties_array.Begin(); it != parties_array.End();
         ++it) {
      if (it->IsString()) {
        std::string party_name = it->GetString();
        cluster_def->parties.push_back(party_name);
      } else {
        SPDLOG_ERROR("cluster_def 'parties' array contains non-string element");
        return nullptr;
      }
    }
  } else {
    SPDLOG_ERROR("cluster_def missing or invalid 'parties' field");
    return nullptr;
  }
  // Parse services
  if (doc.HasMember("services") && doc["services"].IsObject()) {
    const auto& services_obj = doc["services"];
    for (auto it = services_obj.MemberBegin(); it != services_obj.MemberEnd();
         ++it) {
      if (!it->name.IsString() || !it->value.IsObject()) {
        SPDLOG_ERROR("cluster_def 'services' has invalid party entry");
        return nullptr;
      }
      std::string party_name = it->name.GetString();
      const auto& party_services = it->value;
      std::map<std::string, Service> service_map;
      for (auto service_it = party_services.MemberBegin();
           service_it != party_services.MemberEnd(); ++service_it) {
        if (!service_it->name.IsString() || !service_it->value.IsObject()) {
          SPDLOG_ERROR("cluster_def party '{}' has invalid service entry",
                       party_name);
          return nullptr;
        }
        std::string service_name = service_it->name.GetString();
        const auto& service_obj = service_it->value;
        Service service;
        service.name = service_name;
        // Parse service fields
        if (service_obj.HasMember("name") && service_obj["name"].IsString()) {
          service.name = service_obj["name"].GetString();
        }
        if (service_obj.HasMember("host") && service_obj["host"].IsString()) {
          service.host = service_obj["host"].GetString();
        } else {
          SPDLOG_ERROR("service '{}' missing 'host' field", service_name);
          return nullptr;
        }
        if (service_obj.HasMember("port") && service_obj["port"].IsString()) {
          service.port = service_obj["port"].GetString();
        } else {
          SPDLOG_ERROR("service '{}' missing 'port' field", service_name);
          return nullptr;
        }
        if (service_obj.HasMember("scope") && service_obj["scope"].IsString()) {
          service.scope = service_obj["scope"].GetString();
        }
        if (service_obj.HasMember("protocol") &&
            service_obj["protocol"].IsString()) {
          service.protocol = service_obj["protocol"].GetString();
        }
        service_map[service_name] = std::move(service);
      }
      cluster_def->services[party_name] = std::move(service_map);
    }
  } else {
    SPDLOG_ERROR("cluster_def missing or invalid 'services' field");
    return nullptr;
  }
  return cluster_def;
}

std::unique_ptr<scql::pb::ScqlConfig> ParseScqlConfig(
    const std::string& scql_config_json) {
  if (scql_config_json.empty()) {
    return nullptr;
  }

  google::protobuf::util::JsonParseOptions options;
  options.ignore_unknown_fields = true;

  auto scql_config = std::make_unique<scql::pb::ScqlConfig>();
  auto status = google::protobuf::util::JsonStringToMessage(
      scql_config_json, scql_config.get(), options);
  if (!status.ok()) {
    SPDLOG_ERROR("Failed to parse scql_config: {}", status.ToString());
    return nullptr;
  }

  return scql_config;
}

std::string GetPartyHost(const ClusterDef& cluster_def,
                         const std::string& party_code,
                         const std::string& job_id) {
  auto services_iter = cluster_def.services.find(party_code);
  if (services_iter == cluster_def.services.end()) {
    SPDLOG_WARN("No services found for party: {}", party_code);
    return "";
  }

  const auto& service_map = services_iter->second;

  // Assume there is only one service
  if (!service_map.empty()) {
    const auto& first_service = service_map.begin()->second;
    if (first_service.host.empty()) {
      SPDLOG_ERROR("No host found for party: {}", party_code);
      return "";
    }
    return first_service.host;
  }

  SPDLOG_ERROR("No services available for party: {}", party_code);
  return "";
}

std::unique_ptr<scql::pb::JobStartParams> BuildJobStartParams(
    const ClusterDef& cluster_def, const scql::pb::ScqlConfig& scql_config,
    const std::string& job_id) {
  auto job_params = std::make_unique<scql::pb::JobStartParams>();
  job_params->set_job_id(job_id);
  job_params->set_party_code(cluster_def.self_party);

  for (int i = 0; i < scql_config.parties_size(); ++i) {
    const auto& party_code = scql_config.parties()[i];
    auto* party_info = job_params->add_parties();
    party_info->set_code(party_code);
    party_info->set_name(party_code);
    party_info->set_rank(i);

    std::string party_host = GetPartyHost(cluster_def, party_code, job_id);
    if (!party_host.empty()) {
      party_info->set_host(party_host);
    } else {
      SPDLOG_ERROR("Failed to get host for party: {}", party_code);
      // Return nullptr to indicate failure
      return nullptr;
    }
  }

  if (scql_config.has_spu_runtime_cfg()) {
    *job_params->mutable_spu_runtime_cfg() = scql_config.spu_runtime_cfg();
  } else {
    // Set default SPU runtime config when none is provided
    spu::RuntimeConfig default_config;
    default_config.protocol = spu::ProtocolKind::SEMI2K;
    default_config.field = spu::FieldType::FM64;
    default_config.sigmoid_mode = spu::RuntimeConfig::SIGMOID_REAL;
    default_config.experimental_enable_colocated_optimization = true;
    spu::populateRuntimeConfig(default_config);
    *job_params->mutable_spu_runtime_cfg() = default_config.ToProto();
  }
  if (!scql_config.time_zone().empty()) {
    job_params->set_time_zone(scql_config.time_zone());
  }
  if (scql_config.has_link_cfg()) {
    *job_params->mutable_link_cfg() = scql_config.link_cfg();
  }
  if (scql_config.has_psi_cfg()) {
    *job_params->mutable_psi_cfg() = scql_config.psi_cfg();
  }
  if (scql_config.has_log_cfg()) {
    *job_params->mutable_log_cfg() = scql_config.log_cfg();
  }

  return job_params;
}

}  // namespace scql::engine::util
