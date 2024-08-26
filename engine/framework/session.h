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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "libspu/core/context.h"
#include "libspu/device/symbol_table.h"
#include "yacl/link/link.h"

#include "engine/datasource/datasource_adaptor_mgr.h"
#include "engine/datasource/router.h"
#include "engine/framework/party_info.h"
#include "engine/framework/tensor_table.h"
#include "engine/util/logging.h"
#include "engine/util/psi_detail_logger.h"

#include "api/common.pb.h"
#include "api/engine.pb.h"

namespace scql::engine {

// The normal state transition process is:
//   INITIALIZED -> RUNNING -> SUCCEEDED/FAILED
// When the user manually terminates the query, the transition process will be:
//   INITIALIZED -> RUNNING -> ABORTING -> FAILED
enum class SessionState {
  INITIALIZED = 0,
  RUNNING = 1,
  ABORTING = 2,
  SUCCEEDED = 3,
  FAILED = 4,
};

pb::JobState ConvertSessionStateToJobState(SessionState state);

struct LinkConfig {
  uint32_t link_recv_timeout_ms = 30 * 1000;  // 30s
  size_t link_throttle_window_size = 0;
  size_t link_chunked_send_parallel_size = 8;
  yacl::link::RetryOptions link_retry_options;
  size_t http_max_payload_size = 1024 * 1024;  // 1M
};

struct PsiConfig {
  // if the value here is 0, it would use the gflags config instead
  int64_t unbalance_psi_ratio_threshold = 0;
  int64_t unbalance_psi_larger_party_rows_count_threshold = 0;
  int32_t psi_curve_type = 0;
};

struct LogConfig {
  bool enable_session_logger_separation = false;
};

struct SessionOptions {
  util::LogOptions log_options;
  LinkConfig link_config;
  PsiConfig psi_config;
  LogConfig log_config;
};

struct StreamingOptions {
  std::filesystem::path dump_file_dir;
  bool batched;
  // if row num is less than this threshold, close streaming mode and keep all
  // data in memory
  size_t streaming_row_num_threshold;
  // if working in streaming mode, max row num in one batch
  size_t batch_row_num;
};

/// @brief Session holds everything needed to run the execution plan.
class Session {
 public:
  explicit Session(const SessionOptions& session_opt,
                   const pb::JobStartParams& params,
                   pb::DebugOptions debug_opts,
                   yacl::link::ILinkFactory* link_factory, Router* router,
                   DatasourceAdaptorMgr* ds_mgr,
                   const std::vector<spu::ProtocolKind>& allowed_spu_protocols);
  ~Session();
  /// @return session id
  std::string Id() const { return id_; }

  std::string TimeZone() const { return time_zone_; }

  const std::string& SelfPartyCode() const { return parties_.SelfPartyCode(); }

  size_t SelfRank() const { return parties_.SelfRank(); }

  // each session has its own logger, it may contain session id in each log
  // message.
  std::shared_ptr<spdlog::logger> GetLogger() const { return logger_; }

  std::shared_ptr<util::PsiDetailLogger> GetPsiLogger() const {
    return psi_logger_;
  }

  Router* GetRouter() const { return router_; }

  DatasourceAdaptorMgr* GetDatasourceAdaptorMgr() const { return ds_mgr_; }

  ssize_t GetPartyRank(const std::string& party_code) const {
    return parties_.GetRank(party_code);
  }

  std::shared_ptr<yacl::link::Context> GetLink() const { return lctx_; }

  TensorTable* GetTensorTable() const { return tensor_table_.get(); }

  spu::SPUContext* GetSpuContext() const { return spu_ctx_.get(); }

  spu::device::SymbolTable* GetDeviceSymbols() { return &device_symbols_; }

  SessionState GetState() const { return state_.load(); }

  void SetState(SessionState new_state) { state_.store(new_state); }

  // compare and swap state_ to avoid race condition
  bool CASState(SessionState old_state, SessionState new_state) {
    return state_.compare_exchange_strong(old_state, new_state);
  }

  std::chrono::time_point<std::chrono::system_clock> GetStartTime() const {
    return start_time_;
  }

  void SetAffectedRows(int64_t affected_rows) {
    affected_rows_ = affected_rows;
  }

  int64_t GetAffectedRows() const { return affected_rows_; }

  void SetNodesCount(int32_t nodes_count) { nodes_count_ = nodes_count; }

  int32_t GetNodesCount() const { return nodes_count_; }

  void IncExecutedNodes() { ++executed_nodes_; }

  void SetExecutedNodes(int32_t executed_nodes) {
    executed_nodes_ = executed_nodes;
  }

  int32_t GetExecutedNodes() const { return executed_nodes_; }

  auto GetCurrentNodeInfo() {
    std::lock_guard<std::mutex> guard(progress_mutex_);
    return std::make_pair(node_start_time_, current_node_name_);
  }

  void SetCurrentNodeInfo(
      std::chrono::time_point<std::chrono::system_clock> start_time,
      const std::string& name) {
    std::lock_guard<std::mutex> guard(progress_mutex_);
    node_start_time_ = start_time;
    current_node_name_ = name;
  }

  void StorePeerError(const std::string& party_code, const pb::Status& status) {
    std::lock_guard<std::mutex> guard(peer_error_mutex_);
    peer_errors_.emplace_back(party_code, status);
  }

  std::vector<std::pair<std::string, pb::Status>> GetPeerErrors() const {
    std::lock_guard<std::mutex> guard(peer_error_mutex_);
    return peer_errors_;
  }

  void AddPublishResult(std::shared_ptr<pb::Tensor> pb) {
    publish_results_.emplace_back(std::move(pb));
  }

  std::vector<std::shared_ptr<pb::Tensor>> GetPublishResults() const {
    return publish_results_;
  }

  std::shared_ptr<const yacl::link::Statistics> GetLinkStats() const {
    return lctx_->GetStats();
  }

  void MergeDeviceSymbolsFrom(const spu::device::SymbolTable& other);

  TensorPtr StringToHash(const Tensor& string_tensor);

  TensorPtr HashToString(const Tensor& hash_tensor);

  using RefNums = std::vector<std::tuple<std::string, int>>;
  // set origin ref num
  void UpdateRefName(const std::vector<std::string>& input_ref_names,
                     const RefNums& output_ref_nums);

  void DelTensors(const std::vector<std::string>& tensor_names);

  const SessionOptions& GetSessionOptions() const { return session_opt_; }

  StreamingOptions GetStreamingOptions() { return streaming_options_; }
  void SetStreamingOptions(const StreamingOptions& streaming_options) {
    streaming_options_ = streaming_options;
  }
  void EnableStreamingBatched();

 private:
  void InitLink();
  bool ValidateSPUContext();

 private:
  const std::string id_;
  const SessionOptions session_opt_;
  const std::string time_zone_;
  PartyInfo parties_;
  std::atomic<SessionState> state_;
  std::chrono::time_point<std::chrono::system_clock> start_time_;

  yacl::link::ILinkFactory* link_factory_;
  std::shared_ptr<spdlog::logger> logger_;  // session's own logger
  Router* router_;
  DatasourceAdaptorMgr* ds_mgr_;

  // private (plaintext) tensors
  std::unique_ptr<TensorTable> tensor_table_;

  std::shared_ptr<yacl::link::Context> lctx_;
  std::unique_ptr<spu::SPUContext> spu_ctx_;  // SPUContext
  spu::device::SymbolTable device_symbols_;   // spu device symbols table

  absl::flat_hash_map<size_t, std::string> hash_to_string_values_;

  std::vector<std::shared_ptr<pb::Tensor>> publish_results_;
  int64_t affected_rows_ = 0;

  mutable std::mutex mutex_;
  absl::flat_hash_map<std::string, int> tensor_ref_nums_;

  mutable std::mutex peer_error_mutex_;
  std::vector<std::pair<std::string, pb::Status>> peer_errors_;
  std::shared_ptr<util::PsiDetailLogger> psi_logger_ = nullptr;
  pb::DebugOptions debug_opts_;

  const std::vector<spu::ProtocolKind> allowed_spu_protocols_;

  // for progress exposure
  std::atomic_int32_t nodes_count_ = -1;
  std::atomic_int32_t executed_nodes_ = -1;
  mutable std::mutex progress_mutex_;
  std::string current_node_name_;
  std::chrono::time_point<std::chrono::system_clock> node_start_time_;

  // for streaming
  StreamingOptions streaming_options_;
};

std::shared_ptr<spdlog::logger> ActiveLogger(const Session* session);

}  // namespace scql::engine