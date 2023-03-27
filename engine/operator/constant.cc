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

#include "engine/operator/constant.h"

#include "libspu/device/io.h"
#include "libspu/device/symbol_table.h"

#include "engine/core/primitive_builder.h"
#include "engine/core/string_tensor_builder.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string Constant::kOpType("Constant");

const std::string& Constant::Type() const { return kOpType; }

void Constant::Validate(ExecContext* ctx) {
  const auto& outputs = ctx->GetOutput(kOut);
  YACL_ENFORCE(outputs.size() == 1, "Constant output size={} not equal to 1",
               outputs.size());

  const auto& output_status = util::GetTensorStatus(outputs[0]);
  YACL_ENFORCE(output_status == pb::TensorStatus::TENSORSTATUS_PRIVATE ||
                   output_status == pb::TensorStatus::TENSORSTATUS_PUBLIC,
               "Constant output tensor' status should  be private/public");
}

void Constant::Execute(ExecContext* ctx) {
  const auto& scalar_attr = ctx->GetAttribute(kScalarAttr);
  const auto& output = ctx->GetOutput(kOut)[0];
  const auto& output_status = util::GetTensorStatus(output);
  if (output_status == pb::TensorStatus::TENSORSTATUS_PRIVATE) {
    auto tensor = BuildTensorFromScalar(scalar_attr);
    YACL_ENFORCE(tensor != nullptr,
                 "build tensor from scalar attribute failed");

    ctx->GetTensorTable()->AddTensor(output.name(), std::move(tensor));
  } else if (output_status == pb::TensorStatus::TENSORSTATUS_PUBLIC) {
    auto hctx = ctx->GetSession()->GetSpuHalContext();
    spu::device::ColocatedIo cio(hctx);
    util::SpuInfeedHelper infeed_helper(&cio);

    auto tensor = BuildTensorFromScalar(scalar_attr);
    YACL_ENFORCE(tensor != nullptr,
                 "build tensor from scalar attribute failed");
    infeed_helper.InfeedTensorAsPublic(output.name(), *tensor);

    // all parties calculate public value locally to avoid network interaction
    const auto& value_name =
        util::SpuVarNameEncoder::GetValueName(output.name());
    auto nd_arr = cio.hostGetVar(value_name);
    spu::PtBufferView bv(nd_arr.data(),
                         nd_arr.eltype().as<spu::PtTy>()->pt_type(),
                         nd_arr.shape(), nd_arr.strides());

    auto lctx = ctx->GetSession()->GetLink();
    spu::device::IoClient io(lctx->WorldSize(), hctx->rt_config());
    auto shares = io.makeShares(bv, spu::VIS_PUBLIC);

    auto device_symbols = ctx->GetSession()->GetDeviceSymbols();
    device_symbols->setVar(value_name, shares[ctx->GetSession()->SelfRank()]);
#ifdef SCQL_WITH_NULL
    const auto validity_name =
        SpuVarNameEncoder::GetValidityName(output.name());
    auto validity_arr = cio.hostGetVar(validity_name);
    spu::PtBufferView validity_bv(
        validity_arr.data(), validity_arr.eltype().as<spu::PtTy>()->pt_type(),
        validity_arr.shape(), validity_arr.strides());

    auto validity_shares = io.makeShares(validity_bv, spu::VIS_PUBLIC);

    device_symbols->setVar(validity_name,
                           validity_shares[ctx->GetSession()->SelfRank()]);
#endif  // SCQL_WITH_NULL
  }
}

std::shared_ptr<Tensor> Constant::BuildTensorFromScalar(
    const pb::AttributeValue& scalar_attr) {
  std::shared_ptr<Tensor> result;
  const auto& pb_tensor = scalar_attr.t();
  // check pb_tensor
  switch (pb_tensor.elem_type()) {
    case pb::PrimitiveDataType::STRING: {
      StringTensorBuilder builder;
      auto& scalar = pb_tensor.ss();
      YACL_ENFORCE(scalar.ss_size() == 1, "scalar size={} not equal 1",
                   scalar.ss_size());
      builder.Append(scalar.ss(0));
      builder.Finish(&result);
      break;
    }
    case pb::PrimitiveDataType::BOOL: {
      BooleanTensorBuilder builder;
      auto& scalar = pb_tensor.bs();
      YACL_ENFORCE(scalar.bs_size() == 1, "scalar size={} not equal 1",
                   scalar.bs_size());
      builder.Append(scalar.bs(0));
      builder.Finish(&result);
      break;
    }
    case pb::PrimitiveDataType::FLOAT16:
    case pb::PrimitiveDataType::FLOAT:
    case pb::PrimitiveDataType::DOUBLE: {
      FloatTensorBuilder builder;
      auto& scalar = pb_tensor.fs();
      YACL_ENFORCE(scalar.fs_size() == 1, "scalar size={} not equal 1",
                   scalar.fs_size());
      builder.Append(scalar.fs(0));
      builder.Finish(&result);
      break;
    }
    case pb::PrimitiveDataType::INT8:
    case pb::PrimitiveDataType::INT16:
    case pb::PrimitiveDataType::INT32:
    case pb::PrimitiveDataType::UINT8:
    case pb::PrimitiveDataType::UINT16:
    case pb::PrimitiveDataType::UINT32: {
      Int64TensorBuilder builder;
      auto& scalar = pb_tensor.is();
      YACL_ENFORCE(scalar.is_size() == 1, "scalar size={} not equal 1",
                   scalar.is_size());
      builder.Append(scalar.is(0));
      builder.Finish(&result);
      break;
    }
    case pb::PrimitiveDataType::INT64:
    case pb::PrimitiveDataType::UINT64: {
      Int64TensorBuilder builder;
      auto& scalar = pb_tensor.i64s();
      YACL_ENFORCE(scalar.i64s_size() == 1, "scalar size={} not equal 1",
                   scalar.i64s_size());
      builder.Append(scalar.i64s(0));
      builder.Finish(&result);
      break;
    }
    case pb::PrimitiveDataType::BFLOAT16:
    case pb::PrimitiveDataType::COMPLEX64:
    case pb::PrimitiveDataType::COMPLEX128:
      // fall through;
    default:
      YACL_THROW("not supported elem_type:{}", pb_tensor.elem_type());
      break;
  }
  return result;
}

}  // namespace scql::engine::op
