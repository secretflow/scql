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

#include "engine/util/trace_categories.h"

#include <fstream>

// Reserves internal static storage for our tracing categories.
PERFETTO_TRACK_EVENT_STATIC_STORAGE();

namespace scql::engine::util {

void InitializePerfetto() {
  ::perfetto::TracingInitArgs args;
  args.backends = ::perfetto::kInProcessBackend;
  ::perfetto::Tracing::Initialize(args);
  ::perfetto::TrackEvent::Register();
}

std::unique_ptr<::perfetto::TracingSession> StartTracing(int fd) {
  ::perfetto::TraceConfig cfg;
  cfg.add_buffers()->set_size_kb(102400);
  auto* ds_cfg = cfg.add_data_sources()->mutable_config();
  ds_cfg->set_name("track_event");
  auto tracing_session = ::perfetto::Tracing::NewTrace();
  tracing_session->Setup(cfg, fd);
  tracing_session->StartBlocking();
  return tracing_session;
}

void StopTracing(std::unique_ptr<::perfetto::TracingSession> tracing_session) {
  ::perfetto::TrackEvent::Flush();
  // Stop tracing and read the trace data.
  tracing_session->StopBlocking();
}

}  // namespace scql::engine::util
