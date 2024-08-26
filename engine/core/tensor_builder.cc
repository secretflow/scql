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

#include "engine/core/tensor_builder.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor_constructor.h"

namespace scql::engine {

void TensorBuilder::Finish(std::shared_ptr<Tensor>* out) {
  FinishInternal();

  auto result = arrow::ChunkedArray::Make(chunks_, type_);

  std::shared_ptr<arrow::ChunkedArray> chunked_arr;
  THROW_IF_ARROW_NOT_OK(std::move(result).Value(&chunked_arr));

  *out = TensorFrom(std::move(chunked_arr));
}

void TensorBuilder::FinishInternal() {
  std::shared_ptr<arrow::Array> arr;

  arrow::ArrayBuilder* builder = GetBaseBuilder();
  THROW_IF_ARROW_NOT_OK(builder->Finish(&arr));

  chunks_.push_back(std::move(arr));
}
}  // namespace scql::engine