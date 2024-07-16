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

#include <optional>

#include "engine/framework/session.h"

#include "api/core.pb.h"

namespace scql::engine {

/// @brief Operator Execute Context
class ExecContext {
 public:
  ExecContext(const pb::ExecNode& node, Session* session);

  TensorTable* GetTensorTable() const { return session_->GetTensorTable(); }

  Router* GetDatasourceRouter() const { return session_->GetRouter(); }

  DatasourceAdaptorMgr* GetDatasourceAdaptorMgr() const {
    return session_->GetDatasourceAdaptorMgr();
  }

  Session* GetSession() const { return session_; }

  std::shared_ptr<spdlog::logger> GetActiveLogger() const;

  // interface for ExecNode
  const std::string& GetNodeName() const;

  const std::string& GetOpType() const;
  // Input/Output
  const RepeatedPbTensor& GetInput(const std::string& name) const;

  const RepeatedPbTensor& GetOutput(const std::string& name) const;

  // Input/Output
  RepeatedPbTensor GetInputTensors() const;

  RepeatedPbTensor GetOutputTensors() const;

  // attributes
  // get attribute value by name
  const pb::AttributeValue& GetAttribute(const std::string& name) const;
  std::vector<std::string> GetStringValuesFromAttribute(
      const std::string& name) const;
  std::string GetStringValueFromAttribute(const std::string& name) const;
  int64_t GetInt64ValueFromAttribute(const std::string& name) const;
  bool GetBooleanValueFromAttribute(const std::string& name) const;
  bool HasAttribute(const std::string& name) const {
    return node_.attributes().count(name) > 0;
  }
  std::optional<std::vector<std::string>> TryGetStringValuesFromAttribute(
      const std::string& name) const;
  std::optional<std::string> TryGetStringValueFromAttribute(
      const std::string& name) const;
  std::optional<int64_t> TryGetInt64ValueFromAttribute(
      const std::string& name) const;
  std::optional<bool> TryGetBooleanValueFromAttribute(
      const std::string& name) const;

 private:
  const pb::ExecNode& node_;
  Session* session_;
};

}  // namespace scql::engine