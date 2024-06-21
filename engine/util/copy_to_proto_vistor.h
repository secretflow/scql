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

#include "arrow/array.h"
#include "yacl/base/exception.h"

#include "api/core.pb.h"

namespace scql::engine::util {

/// @brief CopyToProtoVistor only copy array's value to pb::Tensor.
class CopyToProtoVistor {
 public:
  CopyToProtoVistor() = delete;

  explicit CopyToProtoVistor(pb::Tensor* to_tensor, bool contain_null)
      : to_proto_(to_tensor), contain_null_(contain_null) {
    YACL_ENFORCE(to_proto_, "to_proto_ can not be null.");
  }

  template <typename T>
  arrow::Status Visit(const T& array) {
    return arrow::Status::NotImplemented(
        fmt::format("type {} is not implemented in CopyToProtoVistor",
                    array.type()->name()));
  }

  arrow::Status Visit(const arrow::LargeStringArray& array) {
    for (int64_t i = 0; i < array.length(); i++) {
      to_proto_->add_string_data(array.GetString(i));
      SetValidityIfNeeded(array.IsValid(i));
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::BooleanArray& array) {
    for (int64_t i = 0; i < array.length(); i++) {
      to_proto_->add_bool_data(array.GetView(i));
      SetValidityIfNeeded(array.IsValid(i));
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::NumericArray<arrow::FloatType>& array) {
    for (int64_t i = 0; i < array.length(); i++) {
      to_proto_->add_float_data(array.GetView(i));
      SetValidityIfNeeded(array.IsValid(i));
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::NumericArray<arrow::DoubleType>& array) {
    for (int64_t i = 0; i < array.length(); i++) {
      to_proto_->add_double_data(array.GetView(i));
      SetValidityIfNeeded(array.IsValid(i));
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::NumericArray<arrow::Int32Type>& array) {
    for (int64_t i = 0; i < array.length(); i++) {
      to_proto_->add_int32_data(array.GetView(i));
      SetValidityIfNeeded(array.IsValid(i));
    }
    return arrow::Status::OK();
  }

  // store uint32 to int64
  arrow::Status Visit(const arrow::NumericArray<arrow::UInt32Type>& array) {
    for (int64_t i = 0; i < array.length(); i++) {
      to_proto_->add_int64_data(array.GetView(i));
      SetValidityIfNeeded(array.IsValid(i));
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::NumericArray<arrow::Int64Type>& array) {
    for (int64_t i = 0; i < array.length(); i++) {
      to_proto_->add_int64_data(array.GetView(i));
      SetValidityIfNeeded(array.IsValid(i));
    }
    return arrow::Status::OK();
  }

  // NOTE: cast uint64 to int64
  arrow::Status Visit(const arrow::NumericArray<arrow::UInt64Type>& array) {
    for (int64_t i = 0; i < array.length(); i++) {
      if (array.GetView(i) > INT64_MAX) {
        return arrow::Status::Invalid(
            fmt::format("overflow while casting uint64 to int64, number#{}={}",
                        i, array.GetView(i)));
      }
      to_proto_->add_int64_data(array.GetView(i));
      SetValidityIfNeeded(array.IsValid(i));
    }
    return arrow::Status::OK();
  }

 private:
  void SetValidityIfNeeded(bool valid) {
    if (contain_null_) {
      to_proto_->add_data_validity(valid);
    }
  }

 private:
  pb::Tensor* to_proto_;
  bool contain_null_;
};

}  // namespace scql::engine::util
