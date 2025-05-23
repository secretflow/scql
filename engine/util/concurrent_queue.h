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

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>
#include <utility>

#include "yacl/base/exception.h"

namespace scql::engine::util {
template <typename T>
class SimpleChannel {
 public:
  explicit SimpleChannel(size_t capacity) : capacity_(capacity) {}
  ~SimpleChannel() {}

  void Push(T& item) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      if (closed_) {
        cond_.notify_all();
        YACL_THROW("send data to a closed queue");
      }
      while (queue_.size() >= capacity_) {
        cond_.wait(lock, [&] { return queue_.size() < capacity_; });
      }
      queue_.push(item);
    }
    cond_.notify_all();
  }

  void Push(T&& item) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      if (closed_) {
        cond_.notify_all();
        YACL_THROW("send data to a closed queue");
      }
      while (queue_.size() >= capacity_) {
        cond_.wait(lock, [&] { return queue_.size() < capacity_; });
      }
      queue_.push(std::forward<T>(item));
    }

    cond_.notify_all();
  }

  std::optional<T> Pop() {
    T item;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      while (queue_.empty() && !closed_) {
        cond_.wait(lock, [&] { return !queue_.empty() || closed_; });
      }
      // return empty item if queue is closed and queue is empty
      if (closed_ && queue_.empty()) {
        return std::nullopt;
      }
      item = queue_.front();
      queue_.pop();
    }
    cond_.notify_all();
    return item;
  }

  // close queue, if a queue is empty, all Pop() will return empty item.
  void Close() {
    if (closed_) {
      YACL_THROW("close a closed queue");
    }
    std::unique_lock<std::mutex> lock(mutex_);
    closed_ = true;
    cond_.notify_all();
  }
  bool IsClosed() { return closed_; }

 private:
  std::atomic_bool closed_{false};
  std::mutex mutex_;
  std::condition_variable cond_;
  size_t capacity_;
  std::queue<T> queue_;
};
}  // namespace scql::engine::util
