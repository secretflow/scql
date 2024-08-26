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

#include "engine/util/time_util.h"

#include <ctime>

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/exec.h"
#include "spdlog/spdlog.h"
#include "yacl/base/exception.h"

#include "engine/core/tensor_constructor.h"
namespace scql::engine::util {

std::string ConvertEpochToStr(time_t epoch) {
  struct tm date_time;
  auto* date_ptr = gmtime_r(&epoch, &date_time);
  YACL_ENFORCE(date_ptr != nullptr,
               "gmtime_r failed, errno: {}, error message: {}", errno,
               strerror(errno));
  char buf[20];
  strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &date_time);
  std::string date_str(buf);
  return date_str;
}

int64_t TimeZoneToSeconds(const std::string& time_zone) {
  size_t length = time_zone.length();
  YACL_ENFORCE(length == 6, "unsupported time zone: {}", time_zone);

  int sign = time_zone[0] == '+' ? 1 : (time_zone[0] == '-' ? -1 : 0);
  YACL_ENFORCE(sign != 0, "unsupported time zone: {}", time_zone);
  YACL_ENFORCE(std::isdigit(time_zone[1]) && std::isdigit(time_zone[2]) &&
                   time_zone[3] == ':' && std::isdigit(time_zone[4]) &&
                   std::isdigit(time_zone[5]),
               "unsupported time zone: {}", time_zone);

  int hours = (time_zone[1] - '0') * 10 + time_zone[2] - '0';
  int minutes = (time_zone[4] - '0') * 10 + time_zone[5] - '0';
  YACL_ENFORCE(minutes <= kMinutePerHour, "unsupported time zone: {}",
               time_zone);
  int seconds = hours * kSecondPerHour + minutes * kSecondsPerMinute;
  YACL_ENFORCE(seconds < kMaxTimeZoneHour * kSecondPerHour,
               "unsupported time zone: {}", time_zone);

  return seconds * sign;
}

std::shared_ptr<Tensor> CompensateTimeZone(
    const std::shared_ptr<Tensor>& from_tensor, const std::string& time_zone) {
  int64_t time_zone_offset_s = TimeZoneToSeconds(time_zone);
  auto chunked_array = from_tensor->ToArrowChunkedArray();
  arrow::Result<arrow::Datum> result = arrow::compute::CallFunction(
      "add", {from_tensor->ToArrowChunkedArray(),
              arrow::MakeScalar(time_zone_offset_s)});

  YACL_ENFORCE(result.ok(),
               "caught error while invoking arrow add function: {}",
               result.status().ToString());
  return TensorFrom(result.ValueOrDie().chunked_array());
}

ConvertDatetimeProtoVistor::ConvertDatetimeProtoVistor(pb::Tensor* to_tensor,
                                                       bool contain_null)
    : to_proto_(to_tensor), contain_null_(contain_null) {
  YACL_ENFORCE(to_proto_, "to_proto_ can not be null.");
}

arrow::Status ConvertDatetimeProtoVistor::Visit(
    const arrow::NumericArray<arrow::Int64Type>& array) {
  for (int64_t i = 0; i < array.length(); i++) {
    to_proto_->add_string_data(
        ConvertEpochToStr(static_cast<time_t>(array.GetView(i))));
    if (contain_null_) {
      to_proto_->add_data_validity(array.IsValid(i));
    }
  }
  return arrow::Status::OK();
}

std::shared_ptr<Tensor> ConvertDateTimeToInt64Visitor::GetResultTensor() {
  std::shared_ptr<Tensor> result;
  builder_.Finish(&result);
  return result;
}

int64_t ConvertDateTimeToInt64Visitor::DateUnitCountPerSecond(
    arrow::TimeUnit::type unit_type) {
  int64_t unit;
  switch (unit_type) {
    case arrow::TimeUnit::type::SECOND:
      unit = 1;
      break;
    case arrow::TimeUnit::type::MILLI:
      unit = kMillisPerSecond;
      break;
    case arrow::TimeUnit::type::MICRO:
      unit = kMicrosPerSecond;
      break;
    case arrow::TimeUnit::type::NANO:
      unit = kNanosPerSecond;
      break;
    default:
      YACL_THROW("unsupported TimeUnit type: {}", fmt::underlying(unit_type));
  }
  return unit;
}

arrow::Status ConvertDateTimeToInt64Visitor::Visit(
    const arrow::NumericArray<arrow::Date32Type>& array) {
  for (int64_t i = 0; i < array.length(); i++) {
    if (array.IsValid(i)) {
      int64_t value = static_cast<int32_t>(array.GetView(i)) * kSecondPerDay;
      builder_.Append(value);
    } else {
      builder_.AppendNull();
    }
  }
  return arrow::Status::OK();
}

arrow::Status ConvertDateTimeToInt64Visitor::Visit(
    const arrow::NumericArray<arrow::Date64Type>& array) {
  for (int64_t i = 0; i < array.length(); i++) {
    if (array.IsValid(i)) {
      int64_t value = static_cast<int64_t>(array.GetView(i)) / kMillisPerSecond;
      builder_.Append(value);
    } else {
      builder_.AppendNull();
    }
  }
  return arrow::Status::OK();
}

arrow::Status ConvertDateTimeToInt64Visitor::Visit(
    const arrow::NumericArray<arrow::TimestampType>& array) {
  int64_t unit = DateUnitCountPerSecond(
      arrow::internal::checked_pointer_cast<arrow::TimestampType>(array.type())
          ->unit());
  for (int64_t i = 0; i < array.length(); i++) {
    if (array.IsValid(i)) {
      int64_t value = static_cast<int64_t>(array.GetView(i));
      builder_.Append(value / unit);
    } else {
      builder_.AppendNull();
    }
  }
  return arrow::Status::OK();
}

}  // namespace scql::engine::util