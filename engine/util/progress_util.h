// Copyright 2024 Ant Group Co., Ltd.
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

#include <cstddef>
#include <mutex>
#include <string>
#include <vector>

#include "fmt/format.h"

namespace scql::engine::util {
class ProgressStats {
 public:
  ProgressStats() = default;

  virtual void IncExecutedNodes() = 0;
  virtual void IncExecutedBatch() = 0;
  virtual void IncExecutedPipeline() = 0;
  virtual void SetBatchCntInPipeline(size_t cnt) = 0;
  virtual size_t GetExecutedNodes() = 0;
  virtual size_t GetNodesCnt() = 0;
  virtual std::string Summary() = 0;

  virtual std::pair<std::chrono::time_point<std::chrono::system_clock>,
                    std::string>
  GetCurrentNodeInfo() = 0;

  virtual void SetCurrentNodeInfo(
      std::chrono::time_point<std::chrono::system_clock> start_time,
      const std::string& name) = 0;
};

class DefaultProgressStats : public ProgressStats {
 public:
  DefaultProgressStats() = default;

  void IncExecutedNodes() override {}
  void IncExecutedBatch() override {}
  void IncExecutedPipeline() override {}
  void SetBatchCntInPipeline(size_t cnt) override {}
  size_t GetNodesCnt() override { return 0; }
  size_t GetExecutedNodes() override { return 0; }
  std::pair<std::chrono::time_point<std::chrono::system_clock>, std::string>
  GetCurrentNodeInfo() override {
    return std::make_pair(std::chrono::system_clock::now(), "");
  }
  std::string Summary() override { return ""; }
  void SetCurrentNodeInfo(
      std::chrono::time_point<std::chrono::system_clock> start_time,
      const std::string& name) override {}
};

class NormalProgressStats : public ProgressStats {
 public:
  explicit NormalProgressStats(size_t total_nodes_cnt)
      : total_nodes_cnt_(total_nodes_cnt) {}

  void IncExecutedNodes() override {
    std::lock_guard<std::mutex> guard(progress_mutex_);
    executed_nodes_cnt_++;
  }
  void IncExecutedBatch() override {}
  void IncExecutedPipeline() override {}
  void SetBatchCntInPipeline(size_t cnt) override {}
  size_t GetNodesCnt() override {
    std::lock_guard<std::mutex> guard(progress_mutex_);
    return total_nodes_cnt_;
  }
  size_t GetExecutedNodes() override {
    std::lock_guard<std::mutex> guard(progress_mutex_);
    return executed_nodes_cnt_;
  }
  std::pair<std::chrono::time_point<std::chrono::system_clock>, std::string>
  GetCurrentNodeInfo() override {
    std::lock_guard<std::mutex> guard(progress_mutex_);
    return std::make_pair(node_start_time_, current_node_name_);
  }
  std::string Summary() override {
    std::lock_guard<std::mutex> guard(progress_mutex_);
    return fmt::format("stages executed {}/{}", executed_nodes_cnt_,
                       total_nodes_cnt_);
  }
  void SetCurrentNodeInfo(
      std::chrono::time_point<std::chrono::system_clock> start_time,
      const std::string& name) override {
    std::lock_guard<std::mutex> guard(progress_mutex_);
    node_start_time_ = start_time;
    current_node_name_ = name;
  }

 private:
  size_t total_nodes_cnt_ = 0;
  size_t executed_nodes_cnt_ = 0;
  mutable std::mutex progress_mutex_;
  std::string current_node_name_;
  std::chrono::time_point<std::chrono::system_clock> node_start_time_;
};

class BatchedProgressStats : public ProgressStats {
 public:
  explicit BatchedProgressStats(const std::vector<size_t>& node_cnt_in_pipeline)
      : node_cnt_in_pipeline_(node_cnt_in_pipeline) {
    for (auto cnt : node_cnt_in_pipeline_) {
      total_nodes_cnt_ += cnt;
    }
  }

  void IncExecutedNodes() override {}
  void IncExecutedBatch() override {
    std::lock_guard<std::mutex> guard(progress_mutex_);
    executed_batch_cnt_in_pipeline_++;
  }
  void IncExecutedPipeline() override {
    std::lock_guard<std::mutex> guard(progress_mutex_);
    executed_nodes_cnt_ += node_cnt_in_pipeline_[executed_pipeline_cnt_];
    executed_pipeline_cnt_++;
    executed_batch_cnt_in_pipeline_ = 0;
    batch_cnt_in_pipeline_ = 0;
  }

  void SetBatchCntInPipeline(size_t cnt) override {
    std::lock_guard<std::mutex> guard(progress_mutex_);
    batch_cnt_in_pipeline_ = cnt;
  }

  size_t GetNodesCnt() override {
    std::lock_guard<std::mutex> guard(progress_mutex_);
    return total_nodes_cnt_;
  }
  size_t GetExecutedNodes() override {
    std::lock_guard<std::mutex> guard(progress_mutex_);
    return executed_nodes_cnt_;
  }
  std::pair<std::chrono::time_point<std::chrono::system_clock>, std::string>
  GetCurrentNodeInfo() override {
    std::lock_guard<std::mutex> guard(progress_mutex_);
    return std::make_pair(node_start_time_, current_node_name_);
  }
  std::string Summary() override {
    return fmt::format(
        "executed pipeline:{}/{} executed batch:{}/{} "
        "node:{} in current pipeline {}",
        executed_pipeline_cnt_, node_cnt_in_pipeline_.size(),
        executed_batch_cnt_in_pipeline_, batch_cnt_in_pipeline_,
        current_node_name_, executed_pipeline_cnt_ + 1);
  }
  void SetCurrentNodeInfo(
      std::chrono::time_point<std::chrono::system_clock> start_time,
      const std::string& name) override {
    std::lock_guard<std::mutex> guard(progress_mutex_);
    node_start_time_ = start_time;
    current_node_name_ = name;
  }

 private:
  std::vector<size_t> node_cnt_in_pipeline_;
  std::size_t executed_pipeline_cnt_ = 0;
  std::size_t executed_batch_cnt_in_pipeline_ = 0;
  size_t total_nodes_cnt_ = 0;
  size_t executed_nodes_cnt_ = 0;
  std::size_t batch_cnt_in_pipeline_ = 0;
  mutable std::mutex progress_mutex_;
  std::string current_node_name_;
  std::chrono::time_point<std::chrono::system_clock> node_start_time_;
};
}  // namespace scql::engine::util