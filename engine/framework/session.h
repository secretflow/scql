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

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "libspu/core/context.h"
#include "libspu/device/symbol_table.h"
#include "yacl/link/link.h"

#include "engine/datasource/datasource_adaptor_mgr.h"
#include "engine/datasource/router.h"
#include "engine/framework/party_info.h"
#include "engine/framework/tensor_table.h"

#include "api/engine.pb.h"

namespace scql::engine {

enum class SessionState {
  IDLE = 0,
  RUNNING = 1,
  // TODO(jingshi) : add state to_be_removed in case of RemoveSession failed.
};

struct SessionOptions {
  uint32_t link_recv_timeout_ms = 30 * 1000;  // 30s
  size_t link_throttle_window_size = 0;
  size_t link_chunked_send_parallel_size = 8;
  yacl::link::RetryOptions link_retry_options;
};

/// @brief Session holds everything needed to run the execution plan.
class Session {
 public:
  explicit Session(const SessionOptions& session_opt,
                   const pb::SessionStartParams& params,
                   yacl::link::ILinkFactory* link_factory,
                   std::shared_ptr<spdlog::logger> logger, Router* router,
                   DatasourceAdaptorMgr* ds_mgr);

  /// @return session id
  std::string Id() const { return id_; }

  std::string TimeZone() const { return time_zone_; }

  // each session has its own logger, it may contain session id in each log
  // message.
  std::shared_ptr<spdlog::logger> GetLogger() const { return logger_; }

  Router* GetRouter() const { return router_; }

  DatasourceAdaptorMgr* GetDatasourceAdaptorMgr() const { return ds_mgr_; }

  const std::string& SelfPartyCode() const { return parties_.SelfPartyCode(); }

  size_t SelfRank() { return parties_.SelfRank(); }

  ssize_t GetPartyRank(const std::string& party_code) const {
    return parties_.GetRank(party_code);
  }

  std::shared_ptr<yacl::link::Context> GetLink() const { return lctx_; }

  TensorTable* GetTensorTable() const { return tensor_table_.get(); }

  spu::SPUContext* GetSpuContext() const { return spu_ctx_.get(); }

  spu::device::SymbolTable* GetDeviceSymbols() { return &device_symbols_; }

  SessionState GetState() { return state_; }

  void SetState(SessionState new_state) { state_ = new_state; }

  std::chrono::time_point<std::chrono::system_clock> GetStartTime() {
    return start_time_;
  }

  void MergeDeviceSymbolsFrom(const spu::device::SymbolTable& other);

  TensorPtr StringToHash(const Tensor& string_tensor);

  TensorPtr HashToString(const Tensor& hash_tensor);

  void AddPublishResult(std::shared_ptr<pb::Tensor> pb) {
    publish_results_.emplace_back(std::move(pb));
  }

  std::vector<std::shared_ptr<pb::Tensor>> GetPublishResults() {
    return publish_results_;
  }

  void SetAffectedRows(int64_t affected_rows) {
    affected_rows_ = affected_rows;
  }

  int64_t GetAffectedRows() const { return affected_rows_; }

  using RefNums = std::vector<std::tuple<std::string, int>>;
  // set origin ref num
  void UpdateRefName(const std::vector<std::string>& input_ref_names,
                     const RefNums& output_ref_nums);

  void DelTensors(const std::vector<std::string>& tensor_names);

  void StorePeerError(const std::string& party_code, const pb::Status& status) {
    std::lock_guard<std::mutex> guard(peer_error_mutex_);
    peer_errors_.emplace_back(party_code, status);
  }

  std::vector<std::pair<std::string, pb::Status>> GetPeerErrors() {
    std::lock_guard<std::mutex> guard(peer_error_mutex_);
    return peer_errors_;
  }

 private:
  void InitLink();

 private:
  const std::string id_;
  const SessionOptions session_opt_;
  const std::string time_zone_;
  PartyInfo parties_;
  SessionState state_;
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

  std::mutex mutex_;
  absl::flat_hash_map<std::string, int> tensor_ref_nums_;

  std::mutex peer_error_mutex_;
  std::vector<std::pair<std::string, pb::Status>> peer_errors_;
};

}  // namespace scql::engine