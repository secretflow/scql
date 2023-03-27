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

#include "engine/framework/registry.h"

#include "yacl/base/exception.h"

namespace scql::engine {

std::unique_ptr<OpRegistry> OpRegistry::Make() {
  return std::unique_ptr<OpRegistry>(new OpRegistry());
}

void OpRegistry::AddOperator(const std::string& type_name,
                             OperatorCreator op_creator) {
  std::lock_guard<std::mutex> guard(lock_);
  auto it = ops_.find(type_name);
  if (it != ops_.end()) {
    YACL_THROW("\"{}\" operator has been already registered", type_name);
  }
  ops_[type_name] = std::move(op_creator);
}

std::unique_ptr<Operator> OpRegistry::GetOperator(
    const std::string& type_name) {
  auto iter = ops_.find(type_name);
  if (iter == ops_.end()) {
    return nullptr;
  }
  return (iter->second)();
}

OpRegistry* GetOpRegistry() {
  static auto g_registry = OpRegistry::Make();
  return g_registry.get();
}

}  // namespace scql::engine