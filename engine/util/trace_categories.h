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
#include <fcntl.h>

#include <cerrno>

#include "perfetto.h"
#include "spdlog/spdlog.h"

#define OPERATOR_CATEGORY "Operator"
#define RPCCALL_CATEGORY "RpcCall"
#define OTHER_CATEGORY "Other"

#define PERFETTO_INTERNAL_SCOPED_FINALIZER_WITH_TRACK(category)                \
  struct PERFETTO_UID(ScopedEvent) {                                           \
    struct EventFinalizer {                                                    \
      EventFinalizer(::perfetto::base::PlatformThreadId id) { id_ = id; }      \
      ~EventFinalizer() { TRACE_EVENT_END(category, ::perfetto::Track(id_)); } \
                                                                               \
      EventFinalizer(const EventFinalizer&) = delete;                          \
      inline EventFinalizer& operator=(const EventFinalizer&) = delete;        \
                                                                               \
      EventFinalizer(EventFinalizer&&) =                                       \
          PERFETTO_INTERNAL_EVENT_FINALIZER_KEYWORD;                           \
      EventFinalizer& operator=(EventFinalizer&&) = delete;                    \
      ::perfetto::base::PlatformThreadId id_;                                  \
    } finalizer;                                                               \
  }

#define PERFETTO_INTERNAL_SCOPED_TRACK_EVENT_WITH_TRACK(category, name, tid, \
                                                        ...)                 \
  TRACE_EVENT_BEGIN(category, name, ::perfetto::Track(tid), ##__VA_ARGS__);  \
  PERFETTO_INTERNAL_SCOPED_FINALIZER_WITH_TRACK(category)                    \
  PERFETTO_UID(scoped_event) { tid }

// Begin a slice which gets automatically closed when going out of scope.
#define TRACE_EVENT_DEFAULT_TRACK(category, name, ...)                       \
  PERFETTO_INTERNAL_SCOPED_TRACK_EVENT_WITH_TRACK(                           \
      category, ::perfetto::internal::DecayEventNameType(name),              \
      ::perfetto::internal::TracingMuxer::Get() == nullptr                   \
          ? 0                                                                \
          : ::perfetto::internal::TracingMuxer::Get()->GetCurrentThreadId(), \
      ##__VA_ARGS__)

PERFETTO_DEFINE_CATEGORIES(
    ::perfetto::Category(OPERATOR_CATEGORY).SetDescription("operators."),
    ::perfetto::Category(RPCCALL_CATEGORY).SetDescription("api call."),
    ::perfetto::Category(OTHER_CATEGORY).SetDescription("other tasks."));

#define COMMUNICATION_COUNTER(stats)                        \
  TRACE_COUNTER(OTHER_CATEGORY, "yacl::link::sent_bytes",   \
                stats->sent_bytes.load());                  \
  TRACE_COUNTER(OTHER_CATEGORY, "yacl::link::recv_bytes",   \
                stats->recv_bytes.load());                  \
  TRACE_COUNTER(OTHER_CATEGORY, "yacl::link::sent_actions", \
                stats->sent_actions.load());                \
  TRACE_COUNTER(OTHER_CATEGORY, "yacl::link::recv_actions", \
                stats->recv_actions.load());

namespace scql::engine::util {

void InitializePerfetto();

std::unique_ptr<::perfetto::TracingSession> StartTracing(int fd);

void StopTracing(std::unique_ptr<::perfetto::TracingSession> tracing_session);

class TracingSessionGuard {
 public:
  explicit TracingSessionGuard(bool enable_trace,
                               const std::string& process_name = "",
                               const std::string& trace_log_path = "")
      : enable_trace_(enable_trace), fd_(-1) {
    if (!enable_trace_) {
      return;
    }
    // setup perfetto tracing session
    scql::engine::util::InitializePerfetto();
    fd_ = open(trace_log_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
    if (fd_ == -1) {
      // disable tracing when failed to write log
      enable_trace_ = false;
      SPDLOG_ERROR("failed to open: {}, errorno: {}", trace_log_path, errno);
      return;
    }
    tracing_session_ = scql::engine::util::StartTracing(fd_);
    // Give a custom name for the traced process.
    ::perfetto::ProcessTrack process_track =
        ::perfetto::ProcessTrack::Current();
    ::perfetto::protos::gen::TrackDescriptor desc = process_track.Serialize();
    desc.mutable_process()->set_process_name(process_name);
    ::perfetto::TrackEvent::SetTrackDescriptor(process_track, desc);
  }

  TracingSessionGuard(const TracingSessionGuard&) = delete;
  TracingSessionGuard& operator=(const TracingSessionGuard&) = delete;

  ~TracingSessionGuard() {
    if (enable_trace_) {
      scql::engine::util::StopTracing(std::move(tracing_session_));
      if (fd_ != -1) {
        close(fd_);
      }
    }
  }

 private:
  bool enable_trace_;
  int fd_;
  std::unique_ptr<::perfetto::TracingSession> tracing_session_;
};

}  // namespace scql::engine::util
