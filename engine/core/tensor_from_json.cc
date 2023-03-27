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

#include "engine/core/tensor_from_json.h"

#include "arrow/ipc/json_simple.h"

#include "engine/core/arrow_helper.h"

namespace scql::engine {

TensorPtr TensorFromJSON(const std::shared_ptr<arrow::DataType>& dtype,
                         const std::string& json) {
  using arrow::ipc::internal::json::ChunkedArrayFromJSON;

  std::shared_ptr<arrow::ChunkedArray> chunked_arr;
  THROW_IF_ARROW_NOT_OK(ChunkedArrayFromJSON(
      dtype, std::vector<std::string>{json}, &chunked_arr));

  return std::make_shared<Tensor>(std::move(chunked_arr));
}

}  // namespace scql::engine
