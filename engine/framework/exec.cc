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

#include "engine/framework/exec.h"

#include "yacl/base/exception.h"

#include "engine/framework/session.h"
#include "engine/util/tensor_util.h"

namespace scql::engine {

ExecContext::ExecContext(const pb::ExecNode& node, Session* session)
    : node_(node), session_(session) {}

std::shared_ptr<spdlog::logger> ExecContext::GetActiveLogger() const {
  return ActiveLogger(session_);
}

const std::string& ExecContext::GetNodeName() const {
  return node_.node_name();
}

const std::string& ExecContext::GetOpType() const { return node_.op_type(); }

const RepeatedPbTensor& ExecContext::GetInput(const std::string& name) const {
  YACL_ENFORCE(node_.inputs().count(name) > 0,
               "the input {} of op {} does not exist", name, node_.op_type());
  return node_.inputs().at(name).tensors();
}

const RepeatedPbTensor& ExecContext::GetOutput(const std::string& name) const {
  YACL_ENFORCE(node_.outputs().count(name) > 0,
               "the output {} of op {} does not exist", name, node_.op_type());
  return node_.outputs().at(name).tensors();
}

RepeatedPbTensor ExecContext::GetInputTensors() const {
  RepeatedPbTensor tensors;
  for (const auto& pair : node_.inputs()) {
    tensors.Add(pair.second.tensors().begin(), pair.second.tensors().end());
  }
  return tensors;
}

RepeatedPbTensor ExecContext::GetOutputTensors() const {
  RepeatedPbTensor tensors;
  for (const auto& pair : node_.outputs()) {
    tensors.Add(pair.second.tensors().begin(), pair.second.tensors().end());
  }
  return tensors;
}

const pb::AttributeValue& ExecContext::GetAttribute(
    const std::string& name) const {
  YACL_ENFORCE(node_.attributes().count(name) > 0,
               "The attribute {} of op {} does not exist", name,
               node_.op_type());
  return node_.attributes().at(name);
}

std::vector<std::string> ExecContext::GetStringValuesFromAttribute(
    const std::string& name) const {
  const auto& attr = GetAttribute(name);
  return util::GetStringValues(attr.t());
}

std::string ExecContext::GetStringValueFromAttribute(
    const std::string& name) const {
  const auto& attr = GetAttribute(name);
  return util::GetStringValue(attr.t());
}

int64_t ExecContext::GetInt64ValueFromAttribute(const std::string& name) const {
  const auto& attr = GetAttribute(name);
  return util::GetInt64Value(attr.t());
}

bool ExecContext::GetBooleanValueFromAttribute(const std::string& name) const {
  const auto& attr = GetAttribute(name);
  return util::GetBooleanValue(attr.t());
}

std::optional<std::vector<std::string>>
ExecContext::TryGetStringValuesFromAttribute(const std::string& name) const {
  if (!HasAttribute(name)) {
    return {};
  }
  const auto& attr = GetAttribute(name);
  return util::GetStringValues(attr.t());
}

std::optional<std::string> ExecContext::TryGetStringValueFromAttribute(
    const std::string& name) const {
  if (!HasAttribute(name)) {
    return {};
  }
  const auto& attr = GetAttribute(name);
  return util::GetStringValue(attr.t());
}

std::optional<int64_t> ExecContext::TryGetInt64ValueFromAttribute(
    const std::string& name) const {
  if (!HasAttribute(name)) {
    return {};
  }
  const auto& attr = GetAttribute(name);
  return util::GetInt64Value(attr.t());
}

std::optional<bool> ExecContext::TryGetBooleanValueFromAttribute(
    const std::string& name) const {
  if (!HasAttribute(name)) {
    return {};
  }
  const auto& attr = GetAttribute(name);
  return util::GetBooleanValue(attr.t());
}

}  // namespace scql::engine