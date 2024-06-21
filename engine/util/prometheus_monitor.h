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

#include <memory>

#include "prometheus/gauge.h"
#include "prometheus/registry.h"

namespace scql::engine::util {

class PrometheusMonitor {
 public:
  static PrometheusMonitor* GetInstance() {
    static PrometheusMonitor t;
    return &t;
  }

  PrometheusMonitor(const PrometheusMonitor&) = delete;
  PrometheusMonitor& operator=(const PrometheusMonitor&) = delete;

  prometheus::Registry* GetRegistry() { return metrics_registry_.get(); }

  // Metrics funcs
  void IncSessionNumberTotal() { stats_->session_number_total.Increment(); }
  void DecSessionNumberTotal() { stats_->session_number_total.Decrement(); }

 private:
  PrometheusMonitor();

  struct Stats {
    prometheus::Gauge& session_number_total;

    explicit Stats(prometheus::Registry& registry);
  };
  std::unique_ptr<Stats> stats_;

  std::unique_ptr<prometheus::Registry> metrics_registry_;
};

}  // namespace scql::engine::util