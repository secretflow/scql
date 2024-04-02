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

#include "engine/services/prometheus_service_impl.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <prometheus/serializer.h>
#include <prometheus/text_serializer.h>

#include "brpc/builtin/prometheus_metrics_service.h"
#include "butil/iobuf.h"
#include "yacl/base/exception.h"

namespace scql::engine {

void MetricsService::default_method(
    ::google::protobuf::RpcController* cntl_base,
    const services::pb::MetricsRequest* request,
    services::pb::MetricsResponse* response,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

  // collect metrics from all collectables
  auto collected_metrics = std::vector<::prometheus::MetricFamily>{};
  for (auto collectable : collectables_) {
    if (!collectable) {
      continue;
    }

    auto&& metrics = collectable->Collect();
    collected_metrics.insert(collected_metrics.end(),
                             std::make_move_iterator(metrics.begin()),
                             std::make_move_iterator(metrics.end()));
  }

  // write response
  cntl->http_response().set_content_type("text/plain");
  {
    butil::IOBufBuilder body;
    auto serializer = std::unique_ptr<::prometheus::Serializer>{
        new ::prometheus::TextSerializer()};
    serializer->Serialize(body, collected_metrics);
    body.move_to(cntl->response_attachment());
  }
  {
    butil::IOBuf brpc_metrics_body;
    // dump brpc metrics
    if (brpc::DumpPrometheusMetricsToIOBuf(&brpc_metrics_body) < 0) {
      cntl->SetFailed("Fail to dump brpc metrics");
      return;
    }
    cntl->response_attachment().append(brpc_metrics_body);
  }
}

void MetricsService::RegisterCollectable(prometheus::Collectable* collectable) {
  collectables_.push_back(collectable);
}

}  // namespace scql::engine
