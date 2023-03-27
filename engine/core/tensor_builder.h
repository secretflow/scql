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

#include "engine/core/tensor.h"

namespace scql::engine {

class TensorBuilder {
 public:
  explicit TensorBuilder(std::shared_ptr<arrow::DataType> type)
      : type_(std::move(type)) {}

  virtual ~TensorBuilder() = default;

  // Return result of builder as a Tensor object.
  void Finish(std::shared_ptr<Tensor>* out);

  // Append a null value to builder
  virtual void AppendNull() = 0;

 protected:
  void FinishInternal();

  virtual arrow::ArrayBuilder* GetBaseBuilder() = 0;

  arrow::ArrayVector chunks_;
  std::shared_ptr<arrow::DataType> type_;
};

}  // namespace scql::engine