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

#include "engine/util/prometheus_monitor.h"

#include "prometheus/detail/builder.h"
#include "yacl/base/exception.h"

namespace scql::engine::util {

PrometheusMonitor::PrometheusMonitor() {
  metrics_registry_ = std::make_unique<prometheus::Registry>();
  YACL_ENFORCE(metrics_registry_);

  // add default metric
  stats_ = std::make_unique<Stats>(*metrics_registry_);
  YACL_ENFORCE(stats_);
}

PrometheusMonitor::Stats::Stats(prometheus::Registry& registry)
    : session_number_total(
          ::prometheus::BuildGauge()
              .Name("engine_concurrent_session_total")
              .Help("The concurrent session nums running in engine")
              .Register(registry)
              .Add({})) {}

}  // namespace scql::engine::util
