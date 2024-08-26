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

#include "engine/operator/filter.h"

#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "libspu/device/io.h"
#include "libspu/device/symbol_table.h"
#include "libspu/kernel/hal/constants.h"
#include "libspu/kernel/hal/public_helper.h"
#include "libspu/kernel/hlo/indexing.h"

#include "engine/core/tensor_constructor.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string Filter::kOpType("Filter");

const std::string& Filter::Type() const { return kOpType; }

void Filter::Validate(ExecContext* ctx) {
  const auto& input_filter = ctx->GetInput(kInFilter);
  YACL_ENFORCE(input_filter.size() == 1, "{} size={} not equal to 1", kInFilter,
               input_filter.size());

  const auto& input_datas = ctx->GetInput(kInData);
  const auto& outputs = ctx->GetOutput(kOut);
  YACL_ENFORCE(input_datas.size() == outputs.size(),
               "{} size={} and {} size={} not equal", kInData,
               input_datas.size(), kOut, outputs.size());
  YACL_ENFORCE(input_datas.size() > 0, "size of ({}) cannot be 0", kInData);

  // all input_datas and outputs must be the same status.
  auto data_status = util::GetTensorStatus(input_datas[0]);
  YACL_ENFORCE(util::AreTensorsStatusMatched(input_datas, data_status));
  YACL_ENFORCE(util::AreTensorsStatusMatched(outputs, data_status));

  // currently supported: 1.filter Private data with PRIVATE/PUBLIC filters
  // 2.filter SECRET data with PUBLIC filter
  auto filter_status = util::GetTensorStatus(input_filter[0]);
  switch (data_status) {
    case pb::TENSORSTATUS_PRIVATE: {
      YACL_ENFORCE(filter_status == pb::TENSORSTATUS_PRIVATE ||
                   filter_status == pb::TENSORSTATUS_PUBLIC);
      break;
    }
    case pb::TENSORSTATUS_SECRET: {
      YACL_ENFORCE(filter_status == pb::TENSORSTATUS_PUBLIC);
      break;
    }
    default:
      YACL_THROW("unsupported data status={}",
                 pb::TensorStatus_Name(data_status));
  }
}

void Filter::Execute(ExecContext* ctx) {
  const auto& filter_pb = ctx->GetInput(kInFilter)[0];
  const auto& data_pbs = ctx->GetInput(kInData);
  const auto& output_pbs = ctx->GetOutput(kOut);
  for (int i = 0; i < data_pbs.size(); ++i) {
    const auto& data_pb = data_pbs[i];
    const auto& output_pb = output_pbs[i];
    if (util::IsTensorStatusMatched(data_pb, pb::TENSORSTATUS_PRIVATE)) {
      FilterPrivate(ctx, filter_pb, data_pb, output_pb.name());
    } else if (util::IsTensorStatusMatched(data_pb, pb::TENSORSTATUS_SECRET)) {
      FilterSecret(ctx, filter_pb, data_pb, output_pb.name());
    } else {
      YACL_THROW("not support to filter data with status={}",
                 pb::TensorStatus_Name(util::GetTensorStatus(data_pb)));
    }
  }
}

// filter status: PUBLIC/PRIVATE, data status: PRIVATE, output status: PRIVATE
void Filter::FilterPrivate(ExecContext* ctx, const pb::Tensor& filter_pb,
                           const pb::Tensor& data_pb,
                           const std::string& output_name) {
  auto filter = GetPrivateOrPublicTensor(ctx, filter_pb);
  YACL_ENFORCE(filter != nullptr, "get null tensor {}", filter_pb.name());
  auto data = ctx->GetTensorTable()->GetTensor(data_pb.name());
  YACL_ENFORCE(data != nullptr, "not find tensor {} in symbol table",
               data_pb.name());

  auto result = arrow::compute::CallFunction(
      "filter", {data->ToArrowChunkedArray(), filter->ToArrowChunkedArray()});
  YACL_ENFORCE(result.ok(), "invoking arrow filter function failed: err_msg={}",
               result.status().ToString());

  ctx->GetTensorTable()->AddTensor(
      output_name, TensorFrom(result.ValueOrDie().chunked_array()));
}

// filter status: PUBLIC, data status: SECRET, output status: SECRET
void Filter::FilterSecret(ExecContext* ctx, const pb::Tensor& filter_pb,
                          const pb::Tensor& data_pb,
                          const std::string& output_name) {
  auto* symbols = ctx->GetSession()->GetDeviceSymbols();
  auto* sctx = ctx->GetSession()->GetSpuContext();

  const auto filter_value_name =
      util::SpuVarNameEncoder::GetValueName(filter_pb.name());
  auto filter_value = symbols->getVar(filter_value_name);
  spu::NdArrayRef filter_arr =
      spu::kernel::hal::dump_public(sctx, filter_value);
  auto filter_span = absl::MakeSpan(static_cast<uint8_t*>(filter_arr.data()),
                                    filter_arr.numel());
#ifdef SCQL_WITH_NULL
  const auto filter_validity_name =
      util::SpuVarNameEncoder::GetValidityName(filter_pb.name());
  auto filter_validity = symbols->getVar(filter_validity_name);
  spu::NdArrayRef filter_validity_arr =
      spu::kernel::hal::dump_public(sctx, filter_validity);
  YACL_ENFORCE(filter_validity_arr.numel() == filter_span.size());
  for (int i = 0; i < filter_validity_arr.numel(); ++i) {
    filter_span[i] = filter_span[i] && filter_validity_arr.at<bool>({i});
  }
#endif  // SCQL_WITH_NULL

  const auto data_value_name =
      util::SpuVarNameEncoder::GetValueName(data_pb.name());
  auto data_value = symbols->getVar(data_value_name);

  auto result = spu::kernel::hlo::FilterByMask(sctx, data_value, filter_span);

  symbols->setVar(util::SpuVarNameEncoder::GetValueName(output_name), result);

#ifdef SCQL_WITH_NULL
  const auto data_validity_name =
      util::SpuVarNameEncoder::GetValidityName(data_pb.name());
  auto data_validity = symbols->getVar(data_validity_name);

  auto result_validity =
      spu::kernel::hlo::FilterByMask(sctx, data_validity, filter_span);

  symbols->setVar(util::SpuVarNameEncoder::GetValidityName(output_name),
                  result_validity);
#endif  // SCQL_WITH_NULL
}

TensorPtr Filter::GetPrivateOrPublicTensor(ExecContext* ctx,
                                           const pb::Tensor& input_pb) {
  TensorPtr ret;
  if (util::IsTensorStatusMatched(input_pb, pb::TENSORSTATUS_PRIVATE)) {
    ret = ctx->GetTensorTable()->GetTensor(input_pb.name());
  } else if (util::IsTensorStatusMatched(input_pb, pb::TENSORSTATUS_PUBLIC)) {
    // read public tensor from spu device symbol table
    auto spu_io = util::SpuOutfeedHelper(ctx->GetSession()->GetSpuContext(),
                                         ctx->GetSession()->GetDeviceSymbols());
    ret = spu_io.DumpPublic(input_pb.name());
  } else {
    YACL_THROW("status (\"{}\") must be either private or public",
               input_pb.name());
  }
  return ret;
}

}  // namespace scql::engine::op