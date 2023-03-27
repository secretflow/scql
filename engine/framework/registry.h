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

#include "engine/framework/operator.h"

namespace scql::engine {

// operator registry
class OpRegistry {
 public:
  using OperatorCreator = std::function<std::unique_ptr<Operator>()>;

  ~OpRegistry() = default;

  // Make a new registry. Most users should use the global registry
  static std::unique_ptr<OpRegistry> Make();

  // Add a new operator creator to the registry
  void AddOperator(const std::string& type_name, OperatorCreator op_creator);

  // Returns a new operator instance of type_name
  std::unique_ptr<Operator> GetOperator(const std::string& type_name);

 private:
  OpRegistry() = default;

  std::mutex lock_;
  // map operator type --> OperatorCreator
  std::unordered_map<std::string, OperatorCreator> ops_;
};

// Return the global operator registry
OpRegistry* GetOpRegistry();

}  // namespace scql::engine