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

#include "engine/util/ndarray_to_arrow.h"

#include "arrow/array/util.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_generate.h"
#include "arrow/visit_type_inline.h"
#include "libspu/core/xt_helper.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/type.h"

namespace scql::engine::util {

namespace {

/// @brief Conversion from spu NdArrayRef to Arrow format.
// Inspired by arrow NumPyConverter:
// https://github.com/apache/arrow/blob/6773da12bbc3c7c5ba96f10f76b31dda3c1c2bc8/cpp/src/arrow/python/numpy_to_arrow.cc#L181
class NdArrayConverter {
 public:
  NdArrayConverter(const spu::NdArrayRef& arr, const spu::NdArrayRef* validity)
      : pool_(arrow::default_memory_pool()),
        arr_(arr),
        validity_(validity),
        pt_type_(spu::PT_INVALID),
        type_(nullptr),
        null_bitmap_(nullptr) {
    length_ = arr_.numel();
  }

  arrow::Status Convert();
  const arrow::ArrayVector& GetResult() const { return out_arrays_; }

  template <typename T>
  arrow::enable_if_primitive_ctype<T, arrow::Status> Visit(const T& type) {
    return VisitNative<T>();
  }
  // default case
  arrow::Status Visit(const arrow::DataType& type) {
    return arrow::Status::NotImplemented(fmt::format(
        "NdArrayConverter doesn't implement <{}> conversion", type.ToString()));
  }

  bool IsStrided() const { return arr_.strides()[0] != 1; }

 protected:
  /// @returns null count
  /// returns -1 if @param[in] validity is invalid
  static int64_t ValidityToBitmap(const spu::NdArrayRef* validity,
                                  int64_t length, uint8_t* bitmap) {
    int64_t null_count = 0;
    try {
      auto xt_validity = spu::xt_adapt<uint8_t>(*validity);
      for (int64_t i = 0; i < length; ++i) {
        if (xt_validity[i] > 0) {
          arrow::bit_util::SetBit(bitmap, i);
        } else {
          ++null_count;
          arrow::bit_util::ClearBit(bitmap, i);
        }
      }
    } catch (...) {
      // invalid validity
      return -1;
    }

    return null_count;
  }

  arrow::Status InitNullBitmap() {
    int64_t null_bytes = arrow::bit_util::BytesForBits(length_);
    ARROW_ASSIGN_OR_RAISE(null_bitmap_,
                          arrow::AllocateResizableBuffer(null_bytes, pool_));
    std::memset(null_bitmap_->mutable_data(), 0,
                static_cast<size_t>(null_bytes));
    null_bitmap_data_ = null_bitmap_->mutable_data();
    return arrow::Status::OK();
  }

  template <typename ArrowType>
  arrow::Status ConvertData(std::shared_ptr<arrow::Buffer>* data) {
    // strides only make sense when numel > 1
    if (arr_.numel() > 1 && IsStrided()) {
      return arrow::Status::NotImplemented(
          "NdArrayConverter doesn't support strided arrays");
    }

    if (pt_type_ == spu::PT_I1) {
      int64_t nbytes = arrow::bit_util::BytesForBits(length_);
      ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateBuffer(nbytes, pool_));

      auto xt_values = spu::xt_adapt<bool>(arr_);
      uint64_t i = 0;
      const auto generator = [&xt_values, &i]() -> bool {
        return xt_values[i++];
      };
      arrow::internal::GenerateBitsUnrolled(buffer->mutable_data(), 0, length_,
                                            generator);

      *data = std::move(buffer);
      return arrow::Status::OK();
    }

    // zero-copy
    *data = std::make_shared<NdArrayRefBuffer>(arr_.buf());
    return arrow::Status::OK();
  }

  arrow::Status PushArray(const std::shared_ptr<arrow::ArrayData>& data) {
    out_arrays_.emplace_back(arrow::MakeArray(data));
    return arrow::Status::OK();
  }

  template <typename ArrowType>
  arrow::Status VisitNative() {
    if (validity_ != nullptr) {
      ARROW_RETURN_NOT_OK(InitNullBitmap());
      null_count_ = ValidityToBitmap(validity_, length_, null_bitmap_data_);
      if (null_count_ == -1) {
        return arrow::Status::Invalid("invalid validity type");
      }
    } else {
      null_count_ = 0;
      null_bitmap_ = nullptr;
      null_bitmap_data_ = nullptr;
    }

    std::shared_ptr<arrow::Buffer> data;
    ARROW_RETURN_NOT_OK(ConvertData<ArrowType>(&data));

    auto arr_data = arrow::ArrayData::Make(type_, length_, {null_bitmap_, data},
                                           null_count_, 0);
    return PushArray(arr_data);
  }

 private:
  arrow::MemoryPool* pool_;

  const spu::NdArrayRef& arr_;
  const spu::NdArrayRef* validity_;
  spu::PtType pt_type_;

  int64_t length_;
  std::shared_ptr<arrow::DataType> type_;

  // Used in visitor pattern
  arrow::ArrayVector out_arrays_;

  std::shared_ptr<arrow::ResizableBuffer> null_bitmap_;
  uint8_t* null_bitmap_data_ = nullptr;
  int64_t null_count_ = 0;
};

arrow::Status NdArrayConverter::Convert() {
  // Question: should we handle 0-dimension (scalar) case here?
  if (arr_.ndim() != 1) {
    return arrow::Status::Invalid(
        "only handle 1-dimensional NdArrayRef arrays");
  }

  pt_type_ = arr_.eltype().as<spu::PtTy>()->pt_type();

  type_ = SpuPtTypeToArrowDataType(pt_type_);

  if (type_ == nullptr) {
    return arrow::Status::Invalid(
        fmt::format("unsupported spu::PtType {}", spu::PtType_Name(pt_type_)));
  }

  // Visit the type to perform conversion
  return arrow::VisitTypeInline(*type_, this);
}

}  // namespace

NdArrayRefBuffer::NdArrayRefBuffer(std::shared_ptr<yacl::Buffer> buf)
    : arrow::Buffer(nullptr, 0), buf_(std::move(buf)) {
  is_mutable_ = false;
  data_ = buf_->data<const uint8_t>();
  size_ = buf_->size();
  capacity_ = size_;
  is_cpu_ = true;
}

std::shared_ptr<arrow::ChunkedArray> NdArrayToArrow(
    const spu::NdArrayRef& arr, const spu::NdArrayRef* validity) {
  NdArrayConverter converter(arr, validity);
  THROW_IF_ARROW_NOT_OK(converter.Convert());
  return std::make_shared<arrow::ChunkedArray>(converter.GetResult());
}
}  // namespace scql::engine::util
