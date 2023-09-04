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

#include "engine/core/primitive_builder.h"

namespace scql::engine {

void BooleanTensorBuilder::Reserve(int64_t additional_elements) {
  THROW_IF_ARROW_NOT_OK(builder_.Reserve(additional_elements));
}

void BooleanTensorBuilder::AppendNull() {
  THROW_IF_ARROW_NOT_OK(builder_.AppendNull());
}

void BooleanTensorBuilder::Append(bool val) {
  THROW_IF_ARROW_NOT_OK(builder_.Append(val));
}

void BooleanTensorBuilder::UnsafeAppend(bool val) {
  builder_.UnsafeAppend(val);
}

void BooleanTensorBuilder::UnsafeAppendNull() { builder_.UnsafeAppendNull(); }

}  // namespace scql::engine