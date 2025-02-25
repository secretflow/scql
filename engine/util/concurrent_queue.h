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

#include <condition_variable>
#include <mutex>
#include <queue>

namespace scql::engine::util {
template <typename T>
class BlockingConcurrentQueue {
 public:
  explicit BlockingConcurrentQueue(size_t capacity) { capacity_ = capacity; }
  ~BlockingConcurrentQueue() {}

  void Push(T& item) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
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
      while (queue_.size() >= capacity_) {
        cond_.wait(lock, [&] { return queue_.size() < capacity_; });
      }
      queue_.push(std::forward<T>(item));
    }
    cond_.notify_all();
  }

  T Pop() {
    T item;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      while (queue_.empty()) {
        cond_.wait(lock, [&] { return !queue_.empty(); });
      }
      item = queue_.front();
      queue_.pop();
    }
    cond_.notify_all();
    return item;
  }

 private:
  std::queue<T> queue_;
  std::mutex mutex_;
  std::condition_variable cond_;
  size_t capacity_;
};
}  // namespace scql::engine::util