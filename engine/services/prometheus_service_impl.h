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

#pragma once

#include <prometheus/registry.h>

#include "engine/services/prometheus_service.pb.h"

namespace scql::engine {

class MetricsService : public services::pb::metrics {
 public:
  MetricsService() = default;

  virtual ~MetricsService() = default;

  void default_method(::google::protobuf::RpcController* cntl_base,
                      const services::pb::MetricsRequest* request,
                      services::pb::MetricsResponse* response,
                      ::google::protobuf::Closure* done) override;

  void RegisterCollectable(prometheus::Collectable* collectable);

 private:
  std::vector<prometheus::MetricFamily> CollectMetrics() const;

 private:
  std::vector<prometheus::Collectable*> collectables_;
};

}  // namespace scql::engine