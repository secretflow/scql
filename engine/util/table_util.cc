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

#include "engine/util/table_util.h"

#include "yacl/base/exception.h"

namespace scql::engine::util {

std::shared_ptr<arrow::Table> ConstructTableFromTensors(
    ExecContext* ctx, const RepeatedPbTensor& input_pbs) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> chunked_arrs;
  int64_t pre_length = 0;
  for (int i = 0; i < input_pbs.size(); ++i) {
    const auto& input_pb = input_pbs[i];
    auto tensor = ctx->GetTensorTable()->GetTensor(input_pb.name());
    YACL_ENFORCE(tensor != nullptr, "get tensor={} from tensor table failed",
                 input_pb.name());
    auto chunked_arr = tensor->ToArrowChunkedArray();
    if (i > 0) {
      YACL_ENFORCE(chunked_arr->length() == pre_length,
                   "input tensors must have the same length");
    }
    pre_length = chunked_arr->length();

    fields.emplace_back(arrow::field(input_pb.name(), chunked_arr->type()));
    chunked_arrs.emplace_back(chunked_arr);
  }

  auto table = arrow::Table::Make(arrow::schema(fields), chunked_arrs);
  return table;
}

}  // namespace scql::engine::util
