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

#include <string_view>

#include "engine/core/tensor_builder.h"

namespace scql::engine {

/// @brief Builder for UTF8 strings tensor
class StringTensorBuilder : public TensorBuilder {
 public:
  StringTensorBuilder() : TensorBuilder(arrow::large_utf8()) {}
  ~StringTensorBuilder() = default;

  void AppendNull() override;

  void Append(std::string_view value);
  void Append(const char* str, std::size_t len);

 private:
  arrow::ArrayBuilder* GetBaseBuilder() override { return &builder_; }

  arrow::LargeStringBuilder builder_;
};

}  // namespace scql::engine