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

#include "arrow/builder.h"
#include "arrow_helper.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor_builder.h"

namespace scql::engine {

class BooleanTensorBuilder : public TensorBuilder {
 public:
  BooleanTensorBuilder() : TensorBuilder(arrow::boolean()) {}

  ~BooleanTensorBuilder() = default;

  void AppendNull() override;
  void Append(bool val);

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

  ~NumericTensorBuilder() = default;

  void AppendNull() override;

  void Append(const value_type val) {
    THROW_IF_ARROW_NOT_OK(builder_.Append(val));
  }

 private:
  arrow::ArrayBuilder* GetBaseBuilder() override { return &builder_; }

  arrow::NumericBuilder<T> builder_;
};

template <typename T>
void NumericTensorBuilder<T>::AppendNull() {
  THROW_IF_ARROW_NOT_OK(builder_.AppendNull());
}

using Int64TensorBuilder = NumericTensorBuilder<arrow::Int64Type>;
using UInt64TensorBuilder = NumericTensorBuilder<arrow::UInt64Type>;
using FloatTensorBuilder = NumericTensorBuilder<arrow::FloatType>;
using DoubleTensorBuilder = NumericTensorBuilder<arrow::DoubleType>;

}  // namespace scql::engine