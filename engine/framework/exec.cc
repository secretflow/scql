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
#include "engine/util/spu_io.h"
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

RepeatedPbTensor ExecContext::GetInputPbs() const {
  RepeatedPbTensor tensors;
  for (const auto& pair : node_.inputs()) {
    tensors.Add(pair.second.tensors().begin(), pair.second.tensors().end());
  }
  return tensors;
}

RepeatedPbTensor ExecContext::GetOutputPbs() const {
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

std::vector<bool> ExecContext::GetBooleanValuesFromAttribute(
    const std::string& name) const {
  const auto& attr = GetAttribute(name);
  return util::GetBooleanValues(attr.t());
}

double ExecContext::GetDoubleValueFromAttribute(const std::string& name) const {
  const auto& attr = GetAttribute(name);
  return util::GetDoubleValue(attr.t());
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

std::optional<double> ExecContext::TryGetDoubleValueFromAttribute(
    const std::string& name) const {
  if (!HasAttribute(name)) {
    return {};
  }
  const auto& attr = GetAttribute(name);
  return util::GetDoubleValue(attr.t());
}

// Tensor access implementations
std::shared_ptr<Tensor> ExecContext::GetInputTensor(
    const std::string& input_name, size_t index) {
  const auto& input_pbs = GetInput(input_name);
  YACL_ENFORCE(index < input_pbs.size(),
               "input index out of range for input: {}", input_name);

  auto tensor = GetTensorTable()->GetTensor(input_pbs[index].name());
  YACL_ENFORCE(tensor, "get private tensor failed, name={}",
               input_pbs[index].name());
  return tensor;
}

std::vector<std::shared_ptr<Tensor>> ExecContext::GetInputTensors(
    const std::string& input_name) {
  const auto& input_pbs = GetInput(input_name);
  std::vector<std::shared_ptr<Tensor>> tensors;
  tensors.reserve(input_pbs.size());

  for (const auto& input_pb : input_pbs) {
    auto tensor = GetTensorTable()->GetTensor(input_pb.name());
    YACL_ENFORCE(tensor, "get private tensor failed, name={}", input_pb.name());
    tensors.push_back(tensor);
  }
  return tensors;
}

void ExecContext::SetOutputTensor(const std::string& output_name,
                                  std::shared_ptr<Tensor> tensor,
                                  size_t index) {
  const auto& output_pbs = GetOutput(output_name);
  YACL_ENFORCE(index < output_pbs.size(),
               "output index out of range for output: {}", output_name);

  GetTensorTable()->AddTensor(output_pbs[index].name(), std::move(tensor));
}

void ExecContext::SetOutputTensors(
    const std::string& output_name,
    const std::vector<std::shared_ptr<Tensor>>& tensors) {
  const auto& output_pbs = GetOutput(output_name);
  YACL_ENFORCE(tensors.size() == output_pbs.size(),
               "tensor count {} doesn't match output count {} for output: {}",
               tensors.size(), output_pbs.size(), output_name);

  for (size_t i = 0; i < tensors.size(); ++i) {
    GetTensorTable()->AddTensor(output_pbs[i].name(), tensors[i]);
  }
}

// SPU Value access implementations
spu::Value ExecContext::GetInputValue(const std::string& input_name,
                                      size_t index) {
  const auto& input_pbs = GetInput(input_name);
  YACL_ENFORCE(index < input_pbs.size(),
               "input index out of range for input: {}", input_name);

  auto device_symbols = GetSession()->GetDeviceSymbols();
  return device_symbols->getVar(
      util::SpuVarNameEncoder::GetValueName(input_pbs[index].name()));
}

std::vector<spu::Value> ExecContext::GetInputValues(
    const std::string& input_name) {
  const auto& input_pbs = GetInput(input_name);
  std::vector<spu::Value> values;
  values.reserve(input_pbs.size());

  auto device_symbols = GetSession()->GetDeviceSymbols();
  for (const auto& input_pb : input_pbs) {
    values.push_back(device_symbols->getVar(
        util::SpuVarNameEncoder::GetValueName(input_pb.name())));
  }
  return values;
}

void ExecContext::SetOutputValue(const std::string& output_name,
                                 const spu::Value& value, size_t index) {
  const auto& output_pbs = GetOutput(output_name);
  YACL_ENFORCE(index < output_pbs.size(),
               "output index out of range for output: {}", output_name);

  auto device_symbols = GetSession()->GetDeviceSymbols();
  device_symbols->setVar(
      util::SpuVarNameEncoder::GetValueName(output_pbs[index].name()), value);
}

void ExecContext::SetOutputValues(const std::string& output_name,
                                  const std::vector<spu::Value>& values) {
  const auto& output_pbs = GetOutput(output_name);
  YACL_ENFORCE(values.size() == output_pbs.size(),
               "value count {} doesn't match output count {} for output: {}",
               values.size(), output_pbs.size(), output_name);

  auto device_symbols = GetSession()->GetDeviceSymbols();
  for (size_t i = 0; i < values.size(); ++i) {
    device_symbols->setVar(
        util::SpuVarNameEncoder::GetValueName(output_pbs[i].name()), values[i]);
  }
}

#ifdef SCQL_WITH_NULL
// SPU Validity access implementations
spu::Value ExecContext::GetInputValidity(const std::string& input_name,
                                         size_t index) {
  const auto& input_pbs = GetInput(input_name);
  YACL_ENFORCE(index < input_pbs.size(),
               "input index out of range for input: {}", input_name);

  auto device_symbols = GetSession()->GetDeviceSymbols();
  return device_symbols->getVar(
      util::SpuVarNameEncoder::GetValidityName(input_pbs[index].name()));
}

std::vector<spu::Value> ExecContext::GetInputValidities(
    const std::string& input_name) {
  const auto& input_pbs = GetInput(input_name);
  std::vector<spu::Value> validities;
  validities.reserve(input_pbs.size());

  auto device_symbols = GetSession()->GetDeviceSymbols();
  for (const auto& input_pb : input_pbs) {
    validities.push_back(device_symbols->getVar(
        util::SpuVarNameEncoder::GetValidityName(input_pb.name())));
  }
  return validities;
}

void ExecContext::SetOutputValidity(const std::string& output_name,
                                    const spu::Value& validity, size_t index) {
  const auto& output_pbs = GetOutput(output_name);
  YACL_ENFORCE(index < output_pbs.size(),
               "output index out of range for output: {}", output_name);

  auto device_symbols = GetSession()->GetDeviceSymbols();
  device_symbols->setVar(
      util::SpuVarNameEncoder::GetValidityName(output_pbs[index].name()),
      validity);
}

void ExecContext::SetOutputValidities(
    const std::string& output_name, const std::vector<spu::Value>& validities) {
  const auto& output_pbs = GetOutput(output_name);
  YACL_ENFORCE(validities.size() == output_pbs.size(),
               "validity count {} doesn't match output count {} for output: {}",
               validities.size(), output_pbs.size(), output_name);

  auto device_symbols = GetSession()->GetDeviceSymbols();
  for (size_t i = 0; i < validities.size(); ++i) {
    device_symbols->setVar(
        util::SpuVarNameEncoder::GetValidityName(output_pbs[i].name()),
        validities[i]);
  }
}
#endif  // SCQL_WITH_NULL

// Status query implementations
pb::TensorStatus ExecContext::GetInputStatus(const std::string& input_name,
                                             size_t index) {
  const auto& input_pbs = GetInput(input_name);
  YACL_ENFORCE(index < input_pbs.size(),
               "input index out of range for input: {}", input_name);

  return util::GetTensorStatus(input_pbs[index]);
}

pb::TensorStatus ExecContext::GetOutputStatus(const std::string& output_name,
                                              size_t index) {
  const auto& output_pbs = GetOutput(output_name);
  YACL_ENFORCE(index < output_pbs.size(),
               "output index out of range for output: {}", output_name);

  return util::GetTensorStatus(output_pbs[index]);
}

std::shared_ptr<Tensor> ExecContext::GetPrivateOrPublicTensor(
    const pb::Tensor& t) {
  std::shared_ptr<Tensor> ret;
  if (util::IsTensorStatusMatched(t, pb::TENSORSTATUS_PUBLIC)) {
    // read public tensor from spu device symbol table
    auto spu_io = util::SpuOutfeedHelper(GetSession()->GetSpuContext(),
                                         GetSession()->GetDeviceSymbols());
    ret = spu_io.DumpPublic(t.name());

    if (t.elem_type() == pb::PrimitiveDataType::STRING) {
      ret = GetSession()->HashToString(*ret);
    }
  } else {
    ret = GetTensorTable()->GetTensor(t.name());
    YACL_ENFORCE(ret, "get private tensor failed, name={}", t.name());
  }
  return ret;
}

}  // namespace scql::engine
