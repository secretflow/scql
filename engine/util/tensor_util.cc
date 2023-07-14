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

#include "engine/util/tensor_util.h"

#include "arrow/visit_array_inline.h"
#include "yacl/base/exception.h"

#include "engine/core/arrow_helper.h"
#include "engine/util/copy_to_proto_vistor.h"

namespace scql::engine::util {

std::string GetStringValue(const pb::Tensor& t) {
  if (t.option() != pb::TensorOptions::VALUE ||
      t.elem_type() != pb::PrimitiveDataType::STRING ||
      t.string_data_size() < 1) {
    YACL_THROW("tensor does not have string value");
  }
  return t.string_data(0);
}

std::vector<std::string> GetStringValues(const pb::Tensor& t) {
  if (t.option() != pb::TensorOptions::VALUE ||
      t.elem_type() != pb::PrimitiveDataType::STRING) {
    YACL_THROW("tensor does not have string values");
  }

  std::vector<std::string> result(t.string_data_size());
  for (int i = 0; i < t.string_data_size(); ++i) {
    result[i] = t.string_data(i);
  }
  return result;
}

int64_t GetInt64Value(const pb::Tensor& t) {
  if (t.option() != pb::TensorOptions::VALUE ||
      t.elem_type() != pb::PrimitiveDataType::INT64 ||
      t.int64_data_size() < 1) {
    YACL_THROW("tensor does not have int64 value");
  }
  return t.int64_data(0);
}

void SetStringValues(pb::Tensor* t, const std::vector<std::string>& values) {
  t->set_option(pb::TensorOptions::VALUE);
  t->set_elem_type(pb::PrimitiveDataType::STRING);
  for (const auto& value : values) {
    t->add_string_data(value);
  }
}

void SetInt64Values(pb::Tensor* t, const std::vector<int64_t>& values) {
  t->set_option(pb::TensorOptions::VALUE);
  t->set_elem_type(pb::PrimitiveDataType::INT64);
  for (const auto& value : values) {
    t->add_int64_data(value);
  }
}

bool GetBooleanValue(const pb::Tensor& t) {
  if (t.option() != pb::TensorOptions::VALUE ||
      t.elem_type() != pb::PrimitiveDataType::BOOL || t.bool_data_size() < 1) {
    YACL_THROW("tensor does not have boolean value");
  }
  return t.bool_data(0);
}

void SetBooleanValues(pb::Tensor* t, const std::vector<bool>& values) {
  t->set_option(pb::TensorOptions::VALUE);
  t->set_elem_type(pb::PrimitiveDataType::BOOL);
  for (const auto& value : values) {
    t->add_bool_data(value);
  }
}

pb::TensorStatus GetTensorStatus(const pb::Tensor& t) {
  return t.annotation().status();
}

bool AreTensorsStatusMatched(
    const google::protobuf::RepeatedPtrField<pb::Tensor>& tensors,
    pb::TensorStatus expect_status) {
  for (const auto& t : tensors) {
    if (!IsTensorStatusMatched(t, expect_status)) {
      return false;
    }
  }
  return true;
}

bool IsTensorStatusMatched(const pb::Tensor& t,
                           pb::TensorStatus expect_status) {
  return GetTensorStatus(t) == expect_status;
}

bool AreTensorsStatusEqualAndOneOf(
    const google::protobuf::RepeatedPtrField<pb::Tensor>& tensors,
    std::vector<pb::TensorStatus> status_set) {
  auto st = GetTensorStatus(tensors[0]);
  for (int i = 1; i < tensors.size(); ++i) {
    if (!IsTensorStatusMatched(tensors[i], st)) {
      return false;
    }
  }
  for (const auto& elem : status_set) {
    if (st == elem) {
      return true;
    }
  }
  return false;
}

void CopyValuesToProto(const std::shared_ptr<Tensor>& from_tensor,
                       pb::Tensor* to_proto) {
  CopyToProtoVistor copy_vistor(to_proto);
  const auto& chunked_arr = from_tensor->ToArrowChunkedArray();
  for (int i = 0; i < chunked_arr->num_chunks(); ++i) {
    THROW_IF_ARROW_NOT_OK(
        arrow::VisitArrayInline(*(chunked_arr->chunk(i)), &copy_vistor));
  }
}

}  // namespace scql::engine::util
