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

#include "engine/operator/case_when.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "libspu/kernel/hlo/basic_binary.h"
#include "libspu/kernel/hlo/const.h"

#include "engine/core/tensor.h"
#include "engine/core/tensor_constructor.h"
#include "engine/framework/exec.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string CaseWhen::kOpType("CaseWhen");
const std::string& CaseWhen::Type() const { return kOpType; }

void CaseWhen::Validate(ExecContext* ctx) {
  const auto& conds = ctx->GetInput(kCond);
  const auto& values = ctx->GetInput(kValue);
  const auto& output = ctx->GetOutput(kOut);
  const auto& value_else = ctx->GetInput(kValueElse);

  // check elem size
  YACL_ENFORCE(!conds.empty(), "cond shouldn't be empty");
  YACL_ENFORCE(conds.size() == values.size(),
               "{} size = {} not equal to {} size = {}", kValue, values.size(),
               kCond, conds.size());
  YACL_ENFORCE(value_else.size() == 1, "{} size = {} not equal to 1",
               kValueElse, value_else.size());
  YACL_ENFORCE(output.size() == 1, "{} size = {} not equal to 1", kOut,
               output.size());

  // check elem type
  for (int i = 0; i < conds.size(); i++) {
    YACL_ENFORCE(conds[i].elem_type() == pb::INT8 ||
                     conds[i].elem_type() == pb::INT32 ||
                     conds[i].elem_type() == pb::INT64 ||
                     conds[i].elem_type() == pb::BOOL,
                 "condition type is {}, only support int8 int32 int64 and bool",
                 pb::PrimitiveDataType_Name(conds[i].elem_type()));
    if (i > 0) {
      YACL_ENFORCE(values[i].elem_type() == values[0].elem_type(),
                   "tensor {} type is {} which should be equal to {}",
                   values[i].name(),
                   pb::PrimitiveDataType_Name(values[i].elem_type()),
                   pb::PrimitiveDataType_Name(values[0].elem_type()));
    }
  }
  YACL_ENFORCE(value_else[0].elem_type() == values[0].elem_type(),
               "tensor {} type is {} which should be equal to {}",
               value_else[0].name(),
               pb::PrimitiveDataType_Name(value_else[0].elem_type()),
               pb::PrimitiveDataType_Name(values[0].elem_type()));

  std::vector<pb::TensorStatus> input_status;
  pb::TensorStatus out_status;
  if (util::OneOfTensorsStatusMatched(conds, pb::TENSORSTATUS_PRIVATE)) {
    input_status = {pb::TENSORSTATUS_PRIVATE};
    out_status = pb::TENSORSTATUS_PRIVATE;
  } else if (util::OneOfTensorsStatusMatched(conds, pb::TENSORSTATUS_SECRET)) {
    input_status = {pb::TENSORSTATUS_SECRET, pb::TENSORSTATUS_PUBLIC};
    out_status = pb::TENSORSTATUS_SECRET;
  } else {
    if (util::OneOfTensorsStatusMatched(values, pb::TENSORSTATUS_SECRET) ||
        util::IsTensorStatusMatched(value_else[0], pb::TENSORSTATUS_SECRET)) {
      input_status = {pb::TENSORSTATUS_SECRET, pb::TENSORSTATUS_PUBLIC};
      out_status = pb::TENSORSTATUS_SECRET;
    } else {
      input_status = {pb::TENSORSTATUS_PUBLIC};
      out_status = pb::TENSORSTATUS_PUBLIC;
    }
  }
  YACL_ENFORCE(util::AreTensorsStatusMatchedOneOf(conds, input_status));
  YACL_ENFORCE(util::AreTensorsStatusMatchedOneOf(values, input_status));
  YACL_ENFORCE(util::AreTensorsStatusMatchedOneOf(value_else, input_status));
  YACL_ENFORCE(util::IsTensorStatusMatched(output[0], out_status));
}

void CaseWhen::Execute(ExecContext* ctx) {
  const auto& output = ctx->GetOutput(kOut);
  auto out_status = util::GetTensorStatus(output[0]);
  return out_status == pb::TensorStatus::TENSORSTATUS_PRIVATE
             ? CaseWhenPrivate(ctx)
             : CaseWhenShare(ctx);
}

void CaseWhen::CaseWhenPrivate(ExecContext* ctx) {
  const auto& cond_pb = ctx->GetInput(kCond);
  const auto& value_pb = ctx->GetInput(kValue);
  const auto& value_else_pb = ctx->GetInput(kValueElse);
  std::vector<arrow::Datum> conds_datum;
  std::vector<arrow::Datum> values_datum;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> conds_array;
  for (int i = 0; i < cond_pb.size(); i++) {
    auto cond = ctx->GetTensorTable()->GetTensor(cond_pb[i].name());
    YACL_ENFORCE(cond, "get tensor {} failed", cond_pb[i].name());
    auto cond_array = cond->ToArrowChunkedArray();
    if (cond->Type() != pb::BOOL) {
      auto cond_nqz = arrow::compute::CallFunction(
          "not_equal", {cond->ToArrowChunkedArray(), arrow::Datum{0}});
      YACL_ENFORCE(cond_nqz.ok(),
                   "caught error while invoking arrow make_struct function: {}",
                   cond_nqz.status().ToString());
      cond_array = cond_nqz.ValueOrDie().chunked_array();
    }
    conds_datum.emplace_back(cond_array);
    auto value = ctx->GetTensorTable()->GetTensor(value_pb[i].name());
    YACL_ENFORCE(value, "get tensor {} failed", value_pb[i].name());
    auto value_array = value->ToArrowChunkedArray();
    values_datum.emplace_back(value_array);
  }

  auto cond_struct = arrow::compute::CallFunction("make_struct", conds_datum);
  YACL_ENFORCE(cond_struct.ok(),
               "caught error while invoking arrow make_struct function: {}",
               cond_struct.status().ToString());
  auto cond_array = cond_struct.ValueOrDie().chunked_array();
  auto value_else = ctx->GetTensorTable()->GetTensor(value_else_pb[0].name());
  YACL_ENFORCE(value_else, "get tensor {} failed", value_else_pb[0].name());
  auto value_else_array = value_else->ToArrowChunkedArray();

  std::vector<arrow::Datum> inputs;
  // cond(1) + values(n) + value_else(1)
  inputs.reserve(values_datum.size() + 2);
  inputs.emplace_back(cond_array);
  inputs.insert(inputs.end(), values_datum.begin(), values_datum.end());
  inputs.emplace_back(value_else_array);

  auto result = arrow::compute::CallFunction("case_when", inputs);
  YACL_ENFORCE(result.ok(),
               "invoking arrow case_when function failed: err_msg={}",
               result.status().ToString());

  const auto& output = ctx->GetOutput(kOut);
  ctx->GetTensorTable()->AddTensor(
      output[0].name(), TensorFrom(result.ValueOrDie().chunked_array()));
}

// condition:
//      origin              transfer
//   |1,  0, -1,  2|     |1, 0, 0, 0, 0|
//   |0, -1,  1,  0|     |0, 1, 0, 0, 0|
//   |1,  0,  1,  1| ==> |1, 0, 0, 0, 0|
//   |0,  0,  0,  1|     |0, 0, 0, 1, 0|
//   |0,  0,  0 , 0|     |0, 0, 0, 0, 1|
std::vector<spu::Value> CaseWhen::TransformConditionData(ExecContext* ctx) {
  const auto& cond_pb = ctx->GetInput(kCond);
  auto* device_symbols = ctx->GetSession()->GetDeviceSymbols();
  auto* sctx = ctx->GetSession()->GetSpuContext();
  std::vector<spu::Value> result;
  result.reserve(cond_pb.size());
  spu::Value cond_pre;
  for (int i = 0; i < cond_pb.size(); i++) {
    auto cond_cur = device_symbols->getVar(
        util::SpuVarNameEncoder::GetValueName(cond_pb[i].name()));
    auto zero = spu::kernel::hlo::Constant(sctx, 0, {cond_cur.shape()});
    if (cond_pb[i].elem_type() != pb::BOOL) {
      cond_cur = spu::kernel::hlo::NotEqual(sctx, cond_cur, zero);
    }
    if (i == 0) {
      cond_pre = cond_cur;
    } else {
      cond_cur = spu::kernel::hlo::Or(sctx, cond_cur, cond_pre);
      cond_pre = cond_cur;
    }
    result.emplace_back(cond_cur);
  }
  auto one_value = spu::kernel::hlo::Constant(sctx, true, {result[0].shape()});
  result.emplace_back(one_value);
  for (size_t i = result.size() - 1; i > 0; i--) {
    result[i] = spu::kernel::hlo::Xor(sctx, result[i], result[i - 1]);
  }
  return result;
}

void CaseWhen::CaseWhenShare(ExecContext* ctx) {
  const auto& value_pb = ctx->GetInput(kValue);
  const auto& value_else_pb = ctx->GetInput(kValueElse);

  auto* device_symbols = ctx->GetSession()->GetDeviceSymbols();
  auto* sctx = ctx->GetSession()->GetSpuContext();
  auto conds = TransformConditionData(ctx);
  spu::Value result_value =
      spu::kernel::hlo::Constant(sctx, 0, {conds[0].shape()});
  for (int i = 0; i < value_pb.size(); i++) {
    auto value = device_symbols->getVar(
        util::SpuVarNameEncoder::GetValueName(value_pb[i].name()));
    auto cur_value = spu::kernel::hlo::Mul(sctx, conds[i], value);
    result_value = spu::kernel::hlo::Add(sctx, result_value, cur_value);
  }
  auto value_else = device_symbols->getVar(
      util::SpuVarNameEncoder::GetValueName(value_else_pb[0].name()));
  auto cur_value = spu::kernel::hlo::Mul(sctx, conds.back(), value_else);
  result_value = spu::kernel::hlo::Add(sctx, result_value, cur_value);
  const auto& output = ctx->GetOutput(kOut);
  device_symbols->setVar(
      util::SpuVarNameEncoder::GetValueName(output[0].name()), result_value);
}

}  // namespace scql::engine::op