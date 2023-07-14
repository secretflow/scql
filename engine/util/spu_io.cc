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
#include "libspu/device/io.h"
#include "libspu/kernel/hal/constants.h"
#include "libspu/kernel/hal/public_helper.h"
#include "libspu/kernel/hlo/casting.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/type.h"
#include "engine/util/ndarray_to_arrow.h"

namespace scql::engine::util {

namespace {

struct SpuPtBufferViewConverter {
  void const* data_ptr = nullptr;

  void Convert(const arrow::Array& array) {
    THROW_IF_ARROW_NOT_OK(arrow::VisitArrayInline(array, this));
  }

  bool HasDataOwnership() { return has_owned_data_; }

  // Release owned data
  yacl::Buffer Release() { return std::move(owned_data_); }

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
    // NOTE(shunde.csd): Arrow boolean array use bitmap to storage values,
    // but spu use one byte to represent bool values.
    // We need expand it.
    owned_data_ = yacl::Buffer(array.length());
    has_owned_data_ = true;

    for (int64_t i = 0; i < array.length(); ++i) {
      owned_data_.data<bool>()[i] = array.GetView(i);
    }
    data_ptr = owned_data_.data();
    return arrow::Status::OK();
  }

 private:
  bool has_owned_data_ = false;
  yacl::Buffer owned_data_;
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

  if (converter.HasDataOwnership()) {
    buffers_.push_back(std::move(converter.Release()));
  } else {
    array_refs_.push_back(array);
  }

  auto value =
      spu::PtBufferView(converter.data_ptr, pt, {array->length()}, {1});

#ifdef SCQL_WITH_NULL
  // NOTE: null_bitmap may be nullptr if all values are valid in array
  // FIXME(shunde.csd): spu does not support single bit bool value, we should
  // expand bit map to byte(uint8) repr.
  const uint8_t* null_bitmap = array->null_bitmap_data();
  int64_t len = (array->length() + 7) / 8;

  auto validity = spu::PtBufferView(static_cast<void const*>(null_bitmap),
                                    spu::PT_U8, {len}, {1});
  return PtView(value, validity);
#endif  // SCQL_WITH_NULL

  return PtView(value);
}

void SpuInfeedHelper::Sync() {
  cio_->sync();
  // release array refs
  array_refs_.clear();
  // release buffers
  buffers_.clear();
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
  return std::make_shared<Tensor>(NdArrayToArrow(arr, &validity));
#else
  return std::make_shared<Tensor>(NdArrayToArrow(arr, nullptr));
#endif  // SCQL_WITH_NULL
}

TensorPtr SpuOutfeedHelper::RevealTo(const std::string& name, size_t rank) {
  const auto& lctx = sctx_->lctx();
  spu::device::IoClient io(lctx->WorldSize(), sctx_->config());

  auto value = symbols_->getVar(SpuVarNameEncoder::GetValueName(name));
  std::string value_content;
  value.toProto().SerializeToString(&value_content);
  auto value_buffers = yacl::link::Gather(
      lctx, yacl::ByteContainerView(value_content), rank, "reveal_value");

#ifdef SCQL_WITH_NULL
  auto validity_val =
      symbols_->getVar(SpuVarNameEncoder::GetValidityName(name));
  std::string validity_content;
  validity_val.toProto().SerializeToString(&validity_content);
  auto validity_buffers = yacl::link::Gather(
      lctx, yacl::ByteContainerView(validity_content), rank, "reveal_validity");
#endif  // SCQL_WITH_NULL

  if (lctx->Rank() != rank) {
    return nullptr;
  }

  std::vector<spu::Value> value_shares;
  for (const auto& buffer : value_buffers) {
    spu::ValueProto value_proto;
    YACL_ENFORCE(value_proto.ParseFromArray(buffer.data(), buffer.size()));
    value_shares.push_back(spu::Value::fromProto(value_proto));
  }
  auto arr = io.combineShares(value_shares);
#ifdef SCQL_WITH_NULL
  std::vector<spu::Value> validity_shares;
  for (const auto& buffer : validity_buffers) {
    spu::ValueProto value_proto;
    YACL_ENFORCE(value_proto.ParseFromArray(buffer.data(), buffer.size()));
    validity_shares.push_back(spu::Value::fromProto(value_proto));
  }
  auto validity = io.combineShares(validity_shares);
  return std::make_shared<Tensor>(NdArrayToArrow(arr, &validity));
#else
  return std::make_shared<Tensor>(NdArrayToArrow(arr, nullptr));
#endif  // SCQL_WITH_NULL
}

}  // namespace scql::engine::util
