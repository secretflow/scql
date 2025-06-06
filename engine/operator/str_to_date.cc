// Copyright 2025 Ant Group Co., Ltd.
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

#include "engine/operator/str_to_date.h"

#include "arrow/array.h"
#include "arrow/compute/api.h"

#include "engine/core/tensor_constructor.h"
#include "engine/framework/exec.h"
#include "engine/util/string_util.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string StrToDate::kOpType("StrToDate");

const std::string& StrToDate::Type() const { return kOpType; }

void StrToDate::Validate(ExecContext* ctx) {
  const auto& left = ctx->GetInput(kLeft);
  const auto& right = ctx->GetInput(kRight);
  const auto& output = ctx->GetOutput(kOut);

  YACL_ENFORCE(left.size() == 1,
               "StrToDate function left input size={} not equal to 1",
               left.size());
  YACL_ENFORCE(right.size() == 1,
               "StrToDate function right input size={} not equal to 1",
               right.size());
  YACL_ENFORCE(output.size() == 1,
               "StrToDate function output size={} not equal to 1",
               output.size());
  YACL_ENFORCE(
      util::IsTensorStatusMatched(left[0],
                                  pb::TensorStatus::TENSORSTATUS_PRIVATE),
      "StrToDate function left input tensor's status should be private");
  YACL_ENFORCE(
      util::IsTensorStatusMatched(right[0],
                                  pb::TensorStatus::TENSORSTATUS_PRIVATE),
      "StrToDate function right input tensor's status should be private");
  YACL_ENFORCE(util::IsTensorStatusMatched(
                   output[0], pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "StrToDate function output tensor's status should be private");
}

void StrToDate::Execute(ExecContext* ctx) {
  const auto& left_pb = ctx->GetInput(kLeft)[0];
  const auto& right_pb = ctx->GetInput(kRight)[0];
  const auto& output_pb = ctx->GetOutput(kOut)[0];

  auto date_str_tensor = ctx->GetTensorTable()->GetTensor(left_pb.name());
  YACL_ENFORCE(date_str_tensor != nullptr,
               "get tensor={} from tensor table failed", left_pb.name());
  auto format_str_tensor = ctx->GetTensorTable()->GetTensor(right_pb.name());
  YACL_ENFORCE(format_str_tensor != nullptr,
               "get tensor={} from tensor table failed", right_pb.name());

  auto arrow_date_strings = date_str_tensor->ToArrowChunkedArray();
  auto arrow_format_string = format_str_tensor->ToArrowChunkedArray();
  YACL_ENFORCE(arrow_format_string->length() == 1,
               "StrToDate: format string tensor should contain exactly one "
               "element, got {}",
               arrow_format_string->length());

  std::string format_pattern;
  if (arrow_format_string->num_chunks() > 0) {
    auto chunk = arrow_format_string->chunk(0);
    auto string_array_chunk =
        std::dynamic_pointer_cast<arrow::LargeStringArray>(chunk);
    YACL_ENFORCE(
        string_array_chunk != nullptr && string_array_chunk->length() > 0,
        "StrToDate: format string tensor chunk is not a valid string array or "
        "is empty");
    format_pattern = string_array_chunk->GetString(0);
  } else {
    YACL_ENFORCE(false, "StrToDate: format string tensor has no chunks");
  }
  format_pattern = util::MySQLDateFormatToArrowFormat(format_pattern);

  arrow::compute::StrptimeOptions strptime_options;
  strptime_options.format = format_pattern;
  strptime_options.unit = arrow::TimeUnit::MILLI;
  strptime_options.error_is_null = true;

  auto result = arrow::compute::CallFunction("strptime", {arrow_date_strings},
                                             &strptime_options);
  YACL_ENFORCE(result.ok(),
               "StrToDate: Arrow 'strptime' function execution failed: %s",
               result.status().ToString());
  auto resultTensor =
      util::ConvertDateTimeToInt64(result.ValueOrDie().chunked_array());
  ctx->GetTensorTable()->AddTensor(output_pb.name(), std::move(resultTensor));
}

}  // namespace scql::engine::op