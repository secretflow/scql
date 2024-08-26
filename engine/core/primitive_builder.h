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

#pragma once

#include <cstdint>

#include "arrow/builder.h"
#include "arrow_helper.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor_builder.h"

namespace scql::engine {

class BooleanTensorBuilder : public TensorBuilder {
 public:
  BooleanTensorBuilder() : TensorBuilder(arrow::boolean()) {}

  ~BooleanTensorBuilder() override = default;

  // Ensure that there is enough space allocated to append the indicated number
  // of elements without any further reallocation. Note that additional_capacity
  // is relative to the current number of elements, so calls to Reserve() which
  // are not interspersed with addition of new elements may not increase the
  // capacity.
  void Reserve(int64_t additional_elements);
  void AppendNull() override;
  void Append(bool val);
  // Note: make sure Reserve is called to attain enough capacity before calling
  // UnsafeAppend
  void UnsafeAppend(bool val);
  // Note: make sure Reserve is called to attain enough capacity before calling
  // UnsafeAppendNull
  void UnsafeAppendNull();

 private:
  arrow::ArrayBuilder* GetBaseBuilder() override { return &builder_; }

  arrow::BooleanBuilder builder_;
};

template <typename T>
class NumericTensorBuilder : public TensorBuilder {
 public:
  using value_type = typename T::c_type;

  NumericTensorBuilder()
      : TensorBuilder(arrow::TypeTraits<T>::type_singleton()) {}

  ~NumericTensorBuilder() override = default;

  // Ensure that there is enough space allocated to append the indicated number
  // of elements without any further reallocation. Note that additional_capacity
  // is relative to the current number of elements, so calls to Reserve() which
  // are not interspersed with addition of new elements may not increase the
  // capacity.
  void Reserve(int64_t additional_elements);

  void AppendNull() override;

  void Append(const value_type val) {
    THROW_IF_ARROW_NOT_OK(builder_.Append(val));
  }

  // Note: make sure Reserve is called to attain enough capacity before calling
  // UnsafeAppend
  void UnsafeAppend(const value_type val) { builder_.UnsafeAppend(val); }

  // Note: make sure Reserve is called to attain enough capacity before calling
  // UnsafeAppendNull
  void UnsafeAppendNull() { builder_.UnsafeAppendNull(); }

 private:
  arrow::ArrayBuilder* GetBaseBuilder() override { return &builder_; }

  arrow::NumericBuilder<T> builder_;
};

template <typename T>
void NumericTensorBuilder<T>::Reserve(int64_t additional_elements) {
  THROW_IF_ARROW_NOT_OK(builder_.Reserve(additional_elements));
}

template <typename T>
void NumericTensorBuilder<T>::AppendNull() {
  THROW_IF_ARROW_NOT_OK(builder_.AppendNull());
}

template <typename T>
TensorPtr FullNumericTensor(size_t count, typename T::c_type value) {
  NumericTensorBuilder<T> builder;
  builder.Reserve(count);
  for (size_t i = 0; i < count; ++i) {
    builder.UnsafeAppend(value);
  }

  TensorPtr result_tensor;
  builder.Finish(&result_tensor);
  return result_tensor;
}

using UInt32TensorBuilder = NumericTensorBuilder<arrow::UInt32Type>;
using Int64TensorBuilder = NumericTensorBuilder<arrow::Int64Type>;
using UInt64TensorBuilder = NumericTensorBuilder<arrow::UInt64Type>;
using FloatTensorBuilder = NumericTensorBuilder<arrow::FloatType>;
using DoubleTensorBuilder = NumericTensorBuilder<arrow::DoubleType>;

}  // namespace scql::engine