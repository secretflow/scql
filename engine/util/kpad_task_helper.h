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

#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "api/engine.pb.h"
#include "api/scql_task.pb.h"

namespace scql::engine::util {

// Service represents a service endpoint in the cluster
struct Service {
  std::string name;      // service name
  std::string host;      // service host address
  std::string port;      // service port number
  std::string scope;     // service scope
  std::string protocol;  // service protocol
};

// ClusterDef defines the cluster configuration for task mode
struct ClusterDef {
  std::string self_party;            // current node's party
  std::vector<std::string> parties;  // all participating parties
  std::map<std::string, std::map<std::string, Service>>
      services;  // party -> service_name -> service
};

// ParseClusterDef parses JSON string into ClusterDef structure
// Returns nullptr if parsing fails
std::unique_ptr<ClusterDef> ParseClusterDef(const std::string& json_str);

// Validate kpad task parameters
// Returns true if all parameters are valid (all empty or all non-empty)
// Returns false and logs error if parameters are inconsistent
bool ValidateKpadTaskParams(const std::string& cluster_def,
                            const std::string& job_id,
                            const std::string& scql_config);

// Parse ScqlConfig from JSON string
// Returns nullptr if parsing fails
std::unique_ptr<scql::pb::ScqlConfig> ParseScqlConfig(
    const std::string& scql_config_json);

// Get host from cluster def for a party
// Returns "host" string, or empty string if not found
std::string GetPartyHost(const ClusterDef& cluster_def,
                         const std::string& party_code,
                         const std::string& job_id);

std::unique_ptr<scql::pb::JobStartParams> BuildJobStartParams(
    const ClusterDef& cluster_def, const scql::pb::ScqlConfig& scql_config,
    const std::string& job_id);

}  // namespace scql::engine::util
