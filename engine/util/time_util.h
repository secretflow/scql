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

#include <string>

#include "arrow/array.h"

#include "engine/core/primitive_builder.h"

#include "api/core.pb.h"

namespace scql::engine::util {

constexpr int64_t kSecondPerDay = 86400;
constexpr int64_t kMillisPerSecond = 1000;
constexpr int64_t kMicrosPerSecond = 1000000;
constexpr int64_t kNanosPerSecond = 1000000000;
constexpr int64_t kMinutePerHour = 60;
constexpr int64_t kSecondsPerMinute = 60;
constexpr int64_t kSecondPerHour = 3600;
constexpr int64_t kMaxTimeZoneHour = 14;

std::string ConvertEpochToStr(time_t epoch);

int64_t TimeZoneToSeconds(const std::string& time_zone);

std::shared_ptr<Tensor> CompensateTimeZone(
    const std::shared_ptr<Tensor>& from_tensor, const std::string& time_zone);

// arrow has no func to convert timestamp to str:
// https://arrow.apache.org/docs/cpp/compute.html#
/// @brief ConvertDatetimeProtoVistor convert timestamp to str and copy array's
/// value to pb::Tensor.
class ConvertDatetimeProtoVistor {
 public:
  ConvertDatetimeProtoVistor() = delete;

  explicit ConvertDatetimeProtoVistor(pb::Tensor* to_tensor, bool contain_null);

  template <typename T>
  arrow::Status Visit(const T& array) {
    return arrow::Status::NotImplemented(
        fmt::format("type {} is not implemented in ConvertDatetimeProtoVistor",
                    array.type()->name()));
  }

  arrow::Status Visit(const arrow::NumericArray<arrow::Int64Type>& array);

 private:
  pb::Tensor* to_proto_;
  bool contain_null_;
};

class ConvertDateTimeToInt64Visitor {
 public:
  ConvertDateTimeToInt64Visitor() = default;

  std::shared_ptr<Tensor> GetResultTensor();

  int64_t DateUnitCountPerSecond(arrow::TimeUnit::type unit_type);

  template <typename T>
  arrow::Status Visit(const T& array) {
    return arrow::Status::NotImplemented(fmt::format(
        "type {} is not implemented in ConvertDateTimeToInt64Visitor",
        array.type()->name()));
  }

  // Date32Type for 32-bit date data (as number of days since UNIX epoch)
  arrow::Status Visit(const arrow::NumericArray<arrow::Date32Type>& array);

  // Date64Type for 64-bit date data (as number of milliseconds since UNIX
  // epoch)
  arrow::Status Visit(const arrow::NumericArray<arrow::Date64Type>& array);

  // TimestampType for datetime data (as number of seconds, milliseconds,
  // microseconds or nanoseconds since UNIX epoch)
  arrow::Status Visit(const arrow::NumericArray<arrow::TimestampType>& array);

 private:
  Int64TensorBuilder builder_;
};

}  // namespace scql::engine::util