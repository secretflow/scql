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

#include "engine/framework/tensor_table.h"

#include "yacl/base/exception.h"

namespace scql::engine {

void TensorTable::AddTensor(const std::string& name,
                            std::shared_ptr<Tensor> tensor) {
  std::lock_guard<std::mutex> guard(lock_);

  auto it = tensors_.find(name);
  if (it != tensors_.end()) {
    YACL_THROW("tensor named \"{}\" already exists in TensorTable", name);
  }
  tensors_[name] = std::move(tensor);
}

void TensorTable::AddOrUpdateTensor(const std::string& name,
                                    std::shared_ptr<Tensor> tensor) {
  std::lock_guard<std::mutex> guard(lock_);
  tensors_[name] = std::move(tensor);
}

std::shared_ptr<Tensor> TensorTable::GetTensor(const std::string& name) const {
  std::lock_guard<std::mutex> guard(lock_);

  auto it = tensors_.find(name);
  if (it != tensors_.end()) {
    return it->second;
  }
  return nullptr;
}

void TensorTable::RemoveTensor(const std::string& name) {
  std::lock_guard<std::mutex> guard(lock_);
  tensors_.erase(name);
}

void TensorTable::Clear() {
  std::lock_guard<std::mutex> guard(lock_);
  tensors_.clear();
}

}  // namespace scql::engine