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

#include "engine/util/spu_io.h"

#include "arrow/array/util.h"
#include "gflags/gflags.h"
#include "libspu/device/io.h"
#include "libspu/kernel/hal/constants.h"
#include "libspu/kernel/hal/public_helper.h"
#include "libspu/kernel/hlo/casting.h"
#include "yacl/link/algorithm/gather.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor_constructor.h"
#include "engine/core/type.h"
#include "engine/util/ndarray_to_arrow.h"

DEFINE_int64(max_chunk_size, 128UL * 1024 * 1024,
             "max chunk size of value proto");

namespace scql::engine::util {

namespace {

struct SpuPtBufferViewConverter {
  void const* data_ptr = nullptr;
  bool is_bitset = false;

  void Convert(const arrow::Array& array) {
    THROW_IF_ARROW_NOT_OK(arrow::VisitArrayInline(array, this));
  }

  template <typename T>
  arrow::Status Visit(const T& array) {
    return arrow::Status::NotImplemented(
        fmt::format("SpuPtBufferViewConverter for type {} is not implemented",
                    array.type()->name()));
  }

  template <typename TYPE>
  arrow::Status Visit(const arrow::NumericArray<TYPE>& array) {
    data_ptr = static_cast<void const*>(array.raw_values());
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::BooleanArray& array) {
    data_ptr = static_cast<void const*>(array.values()->data());
    is_bitset = true;
    return arrow::Status::OK();
  }
};

}  // namespace

std::shared_ptr<arrow::Array> ConcatenateChunkedArray(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_arr) {
  arrow::Result<std::shared_ptr<arrow::Array>> result;

  if (chunked_arr->num_chunks() == 0) {
    result = arrow::MakeEmptyArray(chunked_arr->type());
  } else {
    result = arrow::Concatenate(chunked_arr->chunks());
  }

  YACL_ENFORCE(result.ok(),
               "caught error when try to ConcatenateChunkedArray: {}",
               result.status().ToString());

  return result.ValueOrDie();
}

std::string SpuVarNameEncoder::GetValueName(const std::string& name) {
  return name + ".value";
}

std::string SpuVarNameEncoder::GetValidityName(const std::string& name) {
  return name + ".validity";
}

SpuInfeedHelper::PtView SpuInfeedHelper::ConvertArrowArrayToPtView(
    const std::shared_ptr<arrow::Array>& array) {
  spu::PtType pt = ArrowDataTypeToSpuPtType(array->type());
  YACL_ENFORCE(pt != spu::PT_INVALID, "unsupported arrow data type: {}",
               array->type()->ToString());

  SpuPtBufferViewConverter converter;
  converter.Convert(*array);

  array_refs_.push_back(array);

  auto value = spu::PtBufferView(converter.data_ptr, pt, {array->length()}, {1},
                                 converter.is_bitset);

#ifdef SCQL_WITH_NULL
  // NOTE: null_bitmap may be nullptr if all values are valid in array
  const uint8_t* null_bitmap = array->null_bitmap_data();

  auto validity = spu::PtBufferView(static_cast<void const*>(null_bitmap),
                                    spu::PT_I1, {array->length()}, {1}, true);
  return PtView(value, validity);
#endif  // SCQL_WITH_NULL

  YACL_ENFORCE(array->null_count() == 0, "not support null in spu yet");
  return PtView(value);
}

void SpuInfeedHelper::Sync() {
  cio_->sync();
  // release array refs
  array_refs_.clear();
}

void SpuInfeedHelper::InfeedTensor(const std::string& name,
                                   const Tensor& tensor,
                                   spu::Visibility vtype) {
  // FIXME(shunde.csd): rewrite if spu provides streaming io
  // interface.
  auto array = ConcatenateChunkedArray(tensor.ToArrowChunkedArray());

  PtView pt_view = ConvertArrowArrayToPtView(array);

  const auto val_name = SpuVarNameEncoder::GetValueName(name);
  cio_->hostSetVar(val_name, pt_view.value, vtype);
#ifdef SCQL_WITH_NULL
  const auto validity_name = SpuVarNameEncoder::GetValidityName(name);
  // FIXME(shunde.csd): if pt_view.validity.ptr == nullptr, we should allocate
  // memory for null bitmap and set it with all valid.
  cio_->hostSetVar(validity_name, pt_view.validity, vtype);
#endif  // SCQL_WITH_NULL
}

TensorPtr SpuOutfeedHelper::DumpPublic(const std::string& name) {
  const auto value_name = SpuVarNameEncoder::GetValueName(name);
  if (!symbols_->hasVar(value_name)) {
    return nullptr;
  }
  auto value = symbols_->getVar(value_name);
  spu::NdArrayRef arr = spu::kernel::hal::dump_public(sctx_, value);
#ifdef SCQL_WITH_NULL
  auto validity_val =
      symbols_->getVar(SpuVarNameEncoder::GetValidityName(name));
  auto validity = spu::kernel::hal::dump_public(sctx_, validity_val);
  return TensorFrom(NdArrayToArrow(arr, &validity));
#else
  return TensorFrom(NdArrayToArrow(arr, nullptr));
#endif  // SCQL_WITH_NULL
}

spu::NdArrayRef RevealSpuValue(const spu::SPUContext* sctx,
                               const spu::Value& value, size_t rank) {
  const auto& lctx = sctx->lctx();
  spu::device::IoClient io(lctx->WorldSize(), sctx->config());
  std::string value_content;
  auto value_proto = value.toProto(FLAGS_max_chunk_size);
  std::vector<std::vector<yacl::Buffer>> value_buffers;
  for (const auto& chunk : value_proto.chunks) {
    chunk.SerializeToString(&value_content);
    value_buffers.emplace_back(yacl::link::Gather(
        lctx, yacl::ByteContainerView(value_content), rank, "reveal_value"));
  }
  if (lctx->Rank() != rank) {
    return {};
  }
  std::vector<spu::ValueProto> value_protos(lctx->WorldSize());
  for (auto& v_proto : value_protos) {
    v_proto.meta = value_proto.meta;
    v_proto.chunks =
        std::vector<spu::ValueChunkProto>(value_proto.chunks.size());
  }

  for (size_t i = 0; i < value_buffers.size(); i++) {
    for (size_t j = 0; j < value_buffers[i].size(); j++) {
      spu::ValueChunkProto v;
      YACL_ENFORCE(v.ParseFromArray(value_buffers[i][j].data(),
                                    value_buffers[i][j].size()));
      value_protos[j].chunks[i] = v;
    }
  }
  std::vector<spu::Value> value_shares;
  for (size_t i = 0; i < lctx->WorldSize(); i++) {
    value_shares.push_back(spu::Value::fromProto(value_protos[i]));
  }

  auto pt_type = io.getPtType(value_shares);
  spu::NdArrayRef ret(makePtType(pt_type), value_shares.front().shape());
  spu::PtBufferView pv(ret.data(), pt_type, ret.shape(), ret.strides());
  io.combineShares(value_shares, &pv);
  return ret;
}

TensorPtr SpuOutfeedHelper::RevealTo(const std::string& name, size_t rank) {
  const auto& lctx = sctx_->lctx();
  spu::device::IoClient io(lctx->WorldSize(), sctx_->config());

  auto value = symbols_->getVar(SpuVarNameEncoder::GetValueName(name));
  auto arr = RevealSpuValue(sctx_, value, rank);

#ifdef SCQL_WITH_NULL
  auto validity_val =
      symbols_->getVar(SpuVarNameEncoder::GetValidityName(name));
  auto validity = RevealSpuValue(sctx_, validity_val, rank);
#endif  // SCQL_WITH_NULL

  if (lctx->Rank() != rank) {
    return nullptr;
  }
#ifdef SCQL_WITH_NULL
  return TensorFrom(NdArrayToArrow(arr, &validity));
#else
  return TensorFrom(NdArrayToArrow(arr, nullptr));
#endif  // SCQL_WITH_NULL
}

}  // namespace scql::engine::util
