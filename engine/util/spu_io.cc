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
#include "libspu/core/encoding.h"
#include "libspu/device/io.h"
#include "libspu/kernel/hal/public_helper.h"
#include "libspu/kernel/hal/type_cast.h"
#include "libspu/mpc/common/pv2k.h"

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

spu::DataType GetWiderSpuType(const spu::DataType& t1,
                              const spu::DataType& t2) {
  std::unordered_map<spu::DataType, int> type_index{
      // 1
      {spu::DataType::DT_I1, 1},
      // 8
      {spu::DataType::DT_I8, 2},
      {spu::DataType::DT_U8, 3},
      // 16
      {spu::DataType::DT_I16, 4},
      {spu::DataType::DT_U16, 5},
      {spu::DataType::DT_F16, 6},
      // 32
      {spu::DataType::DT_I32, 7},
      {spu::DataType::DT_U32, 8},
      {spu::DataType::DT_F32, 9},
      // 64
      {spu::DataType::DT_I64, 10},
      {spu::DataType::DT_U64, 11},
      {spu::DataType::DT_F64, 12}};

  if (type_index.find(t1) == type_index.end()) {
    return t2;
  }

  if (type_index.find(t2) == type_index.end()) {
    return t1;
  }

  if (type_index[t1] > type_index[t2]) {
    return t1;
  } else {
    return t2;
  }
}

std::vector<spu::Value> ExpandGroupValueReversely(
    spu::SPUContext* sctx, const std::vector<spu::Value>& inputs,
    const spu::Value& mark) {
  // TODO: Scan api support vector inputs to speed up
  std::vector<spu::Value> ret;
  auto reversed_mark = spu::kernel::hlo::Reverse(sctx, mark, {0});
  for (auto v : inputs) {
    auto reversed_v = util::Scan(
        sctx, spu::kernel::hlo::Reverse(sctx, v, {0}), reversed_mark,
        [&](const spu::Value& lhs_v, const spu::Value& lhs_gm,
            const spu::Value& rhs_v, const spu::Value& rhs_gm) {
          spu::Value new_v = spu::kernel::hlo::Add(
              sctx, lhs_v,
              spu::kernel::hlo::Mul(sctx, lhs_gm,
                                    spu::kernel::hlo::Sub(sctx, rhs_v, lhs_v)));
          spu::Value new_gm = spu::kernel::hlo::Mul(sctx, lhs_gm, rhs_gm);

          return std::make_pair(new_v, new_gm);
        });

    ret.push_back(spu::kernel::hlo::Reverse(sctx, reversed_v, {0}));
  }
  return ret;
}

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

spu::NdArrayRef RevealValueTo(spu::SPUContext* sctx, spu::device::IoClient& io,
                              const spu::Value& value, size_t rank) {
  auto private_value = spu::kernel::hal::reveal_to(sctx, value, rank);
  if (sctx->lctx()->Rank() != rank) {
    return spu::NdArrayRef();
  }
  auto dtype = private_value.dtype();
  auto pt_type = io.getPtType({private_value});
  auto fxp_bits = sctx->getFxpBits();

  spu::NdArrayRef decoded(spu::makePtType(pt_type), private_value.shape());
  spu::PtBufferView pv(decoded.data(), pt_type, decoded.shape(),
                       decoded.strides());

  spu::decodeFromRing(private_value.data(), dtype, fxp_bits, &pv);

  return decoded;
}

TensorPtr SpuOutfeedHelper::RevealTo(const std::string& name, size_t rank) {
  const auto& lctx = sctx_->lctx();

  auto value = symbols_->getVar(SpuVarNameEncoder::GetValueName(name));
  if (value.isPublic()) {
    return DumpPublic(name);
  }

  spu::device::IoClient io(lctx->WorldSize(), sctx_->config());
  auto revealed_value = RevealValueTo(sctx_, io, value, rank);

#ifdef SCQL_WITH_NULL
  auto validity = symbols_->getVar(SpuVarNameEncoder::GetValidityName(name));
  auto revealed_validity = RevealValueTo(sctx_, io, validity, rank);
#endif  // SCQL_WITH_NULL

  if (lctx->Rank() != rank) {
    return nullptr;
  }
#ifdef SCQL_WITH_NULL
  return TensorFrom(NdArrayToArrow(revealed_value, &revealed_validity));
#else
  return TensorFrom(NdArrayToArrow(revealed_value, nullptr));
#endif  // SCQL_WITH_NULL
}

}  // namespace scql::engine::util
