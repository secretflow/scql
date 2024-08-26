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

#include "engine/operator/if.h"

#include <string>
#include <vector>

#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "libspu/kernel/hlo/basic_binary.h"
#include "libspu/kernel/hlo/casting.h"
#include "libspu/kernel/hlo/const.h"

#include "engine/core/tensor_constructor.h"
#include "engine/framework/exec.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string If::kOpType("If");
const std::string& If::Type() const { return kOpType; }

void If::Validate(ExecContext* ctx) {
  const auto& cond = ctx->GetInput(kCond);
  YACL_ENFORCE(cond.size() == 1, "{} size = {} not equal to 1", kCond,
               cond.size());
  const auto& valueFalse = ctx->GetInput(kValueFalse);
  YACL_ENFORCE(valueFalse.size() == 1, "{} size = {} not equal to 1",
               kValueFalse, valueFalse.size());
  const auto& valueTrue = ctx->GetInput(kValueTrue);
  YACL_ENFORCE(valueTrue.size() == 1, "{} size = {} not equal to 1", kValueTrue,
               valueTrue.size());
  const auto& output = ctx->GetOutput(kOut);
  YACL_ENFORCE(output.size() == 1, "{} size = {} not equal to 1", kOut,
               output.size());

  YACL_ENFORCE(cond[0].elem_type() != pb::STRING,
               "unsupported condition type {} for tensor {}",
               pb::PrimitiveDataType_Name(cond[0].elem_type()), cond[0].name());

  std::vector<pb::Tensor> ts = {cond[0], valueFalse[0], valueTrue[0],
                                output[0]};
  if (util::OneOfTensorsStatusMatched({ts.begin(), ts.end()},
                                      pb::TensorStatus::TENSORSTATUS_PRIVATE)) {
    YACL_ENFORCE(util::AreTensorsStatusMatched(
        {ts.begin(), ts.end()}, pb::TensorStatus::TENSORSTATUS_PRIVATE));
  } else if (util::OneOfTensorsStatusMatched(
                 {ts.begin(), ts.end()},
                 pb::TensorStatus::TENSORSTATUS_SECRET)) {
    YACL_ENFORCE(util::IsTensorStatusMatched(
        output[0], pb::TensorStatus::TENSORSTATUS_SECRET));
  }
}

void If::Execute(ExecContext* ctx) {
  const auto& cond = ctx->GetInput(kCond);
  const auto& valueFalse = ctx->GetInput(kValueFalse);
  const auto& valueTrue = ctx->GetInput(kValueTrue);
  const auto& output = ctx->GetOutput(kOut);
  auto out_status = util::GetTensorStatus(output[0]);

  if (out_status == pb::TensorStatus::TENSORSTATUS_PRIVATE) {
    IfPrivate(ctx, cond[0], valueTrue[0], valueFalse[0], output[0].name());
  } else {
    IfSecret(ctx, cond[0], valueTrue[0], valueFalse[0], output[0].name());
  }
}

void If::IfPrivate(ExecContext* ctx, const pb::Tensor& cond_pb,
                   const pb::Tensor& value_true_pb,
                   const pb::Tensor& value_false_pb,
                   const std::string& output_name) {
  auto cond = ctx->GetTensorTable()->GetTensor(cond_pb.name());
  YACL_ENFORCE(cond, "get tensor {} failed", cond_pb.name());
  auto value_true = ctx->GetTensorTable()->GetTensor(value_true_pb.name());
  YACL_ENFORCE(value_true, "get tensor {} failed", value_true_pb.name());
  auto value_false = ctx->GetTensorTable()->GetTensor(value_false_pb.name());
  YACL_ENFORCE(value_false, "get tensor {} failed", value_false_pb.name());
  auto cond_array = cond->ToArrowChunkedArray();
  if (cond->Type() != pb::BOOL) {
    cond_array =
        arrow::compute::CallFunction(
            "not_equal", {cond->ToArrowChunkedArray(), arrow::Datum{0}})
            .ValueOrDie()
            .chunked_array();
  }
  auto result = arrow::compute::CallFunction(
      "if_else", {cond_array, value_true->ToArrowChunkedArray(),
                  value_false->ToArrowChunkedArray()});
  YACL_ENFORCE(result.ok(),
               "invoking arrow if_else function failed: err_msg={}",
               result.status().ToString());
  ctx->GetTensorTable()->AddTensor(
      output_name, TensorFrom(result.ValueOrDie().chunked_array()));
}

void If::IfSecret(ExecContext* ctx, const pb::Tensor& cond_pb,
                  const pb::Tensor& value_true_pb,
                  const pb::Tensor& value_false_pb,
                  const std::string& output_name) {
  auto* device_symbols = ctx->GetSession()->GetDeviceSymbols();
  auto* sctx = ctx->GetSession()->GetSpuContext();

  auto cond = device_symbols->getVar(
      util::SpuVarNameEncoder::GetValueName(cond_pb.name()));
  auto value_true = device_symbols->getVar(
      util::SpuVarNameEncoder::GetValueName(value_true_pb.name()));
  auto value_false = device_symbols->getVar(
      util::SpuVarNameEncoder::GetValueName(value_false_pb.name()));
  auto zero = spu::kernel::hlo::Constant(sctx, 0, {cond.shape()});
  auto cond_equal_zero = spu::kernel::hlo::Equal(sctx, cond, zero);
  auto result_value = spu::kernel::hlo::Add(
      sctx,
      spu::kernel::hlo::Mul(
          sctx, spu::kernel::hlo::Sub(sctx, value_false, value_true),
          cond_equal_zero),
      value_true);
  device_symbols->setVar(util::SpuVarNameEncoder::GetValueName(output_name),
                         result_value);
}

}  // namespace scql::engine::op