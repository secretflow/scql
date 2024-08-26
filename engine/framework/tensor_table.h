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
#include <mutex>
#include <unordered_map>

#include "engine/core/tensor.h"

namespace scql::engine {

class TensorTable {
 public:
  // check tensor not existing before inserting to tensor table
  void AddTensor(const std::string& name, std::shared_ptr<Tensor> tensor);

  // insert to tensor table without checking
  void AddOrUpdateTensor(const std::string& name,
                         std::shared_ptr<Tensor> tensor);

  std::shared_ptr<Tensor> GetTensor(const std::string& name) const;

  void RemoveTensor(const std::string& name);

  void Clear();

 private:
  mutable std::mutex lock_;
  std::unordered_map<std::string, std::shared_ptr<Tensor>> tensors_;
};

}  // namespace scql::engine