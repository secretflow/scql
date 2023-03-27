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

#include "engine/core/string_tensor_builder.h"

#include "arrow_helper.h"

namespace scql::engine {

void StringTensorBuilder::AppendNull() {
  THROW_IF_ARROW_NOT_OK(builder_.AppendNull());
}

void StringTensorBuilder::Append(std::string_view value) {
  THROW_IF_ARROW_NOT_OK(builder_.Append(value));
}

void StringTensorBuilder::Append(const char* str, std::size_t len) {
  THROW_IF_ARROW_NOT_OK(builder_.Append(str, len));
}

}  // namespace scql::engine