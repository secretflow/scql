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
#include "engine/util/spu_io.h"
#include "engine/util/time_util.h"

namespace scql::engine::util {

std::string GetStringValue(const pb::Tensor& t) {
  if (t.option() != pb::TensorOptions::VALUE ||
      t.elem_type() != pb::PrimitiveDataType::STRING ||
      t.string_data_size() < 1) {
    YACL_THROW("tensor does not have string value");
  }
  return t.string_data(0);
}

uint32_t GetScalarUint32(std::shared_ptr<Tensor> t) {
  auto num_array = util::ConcatenateChunkedArray(t->ToArrowChunkedArray());
  auto* num_array_ptr =
      dynamic_cast<const arrow::UInt32Array*>(num_array.get());
  YACL_ENFORCE(num_array_ptr, "cast tensor to uint32_t failed");

  YACL_ENFORCE(num_array_ptr->length() == 1,
               "input tensor should be a scalar, but got {}",
               num_array_ptr->length());
  return num_array_ptr->Value(0);
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

bool AreTensorsStatusMatched(const RepeatedPbTensor& tensors,
                             pb::TensorStatus expect_status) {
  for (const auto& t : tensors) {
    if (!IsTensorStatusMatched(t, expect_status)) {
      return false;
    }
  }
  return true;
}

bool AreTensorsStatusMatchedOneOf(
    const RepeatedPbTensor& tensors,
    const std::vector<pb::TensorStatus>& status_set) {
  for (int i = 1; i < tensors.size(); ++i) {
    auto st = GetTensorStatus(tensors[i]);
    bool matched = false;
    for (const auto& elem : status_set) {
      if (st == elem) {
        matched = true;
        continue;
      }
    }
    if (!matched) {
      return false;
    }
  }
  return true;
}

bool OneOfTensorsStatusMatched(const RepeatedPbTensor& tensors,
                               pb::TensorStatus expect_status) {
  return std::any_of(tensors.begin(), tensors.end(), [&](const pb::Tensor& t) {
    return IsTensorStatusMatched(t, expect_status);
  });
}

bool IsTensorStatusMatched(const pb::Tensor& t,
                           pb::TensorStatus expect_status) {
  return GetTensorStatus(t) == expect_status;
}

bool AreTensorsStatusEqualAndOneOf(const RepeatedPbTensor& tensors,
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
  CopyToProtoVistor copy_vistor(to_proto, from_tensor->GetNullCount() > 0);
  const auto& chunked_arr = from_tensor->ToArrowChunkedArray();
  for (int i = 0; i < chunked_arr->num_chunks(); ++i) {
    THROW_IF_ARROW_NOT_OK(
        arrow::VisitArrayInline(*(chunked_arr->chunk(i)), &copy_vistor));
  }
}

std::shared_ptr<Tensor> ConvertDateTimeToInt64(
    const std::shared_ptr<arrow::ChunkedArray> from_chunked_arr) {
  ConvertDateTimeToInt64Visitor convert_visitor;
  for (int i = 0; i < from_chunked_arr->num_chunks(); ++i) {
    THROW_IF_ARROW_NOT_OK(arrow::VisitArrayInline(*(from_chunked_arr->chunk(i)),
                                                  &convert_visitor));
  }
  return convert_visitor.GetResultTensor();
}

std::shared_ptr<arrow::ChunkedArray> ConvertDateTimeToInt64(
    const std::shared_ptr<arrow::Array> from_arr) {
  ConvertDateTimeToInt64Visitor convert_visitor;
  THROW_IF_ARROW_NOT_OK(arrow::VisitArrayInline(*from_arr, &convert_visitor));
  return convert_visitor.GetResultTensor()->ToArrowChunkedArray();
}

void ConvertDateTimeAndCopyValuesToProto(
    const std::shared_ptr<Tensor>& from_tensor, pb::Tensor* to_proto) {
  ConvertDatetimeProtoVistor convert_copy_vistor(to_proto,
                                                 from_tensor->GetNullCount());
  const auto& chunked_arr = from_tensor->ToArrowChunkedArray();
  for (int i = 0; i < chunked_arr->num_chunks(); ++i) {
    THROW_IF_ARROW_NOT_OK(arrow::VisitArrayInline(*(chunked_arr->chunk(i)),
                                                  &convert_copy_vistor));
  }
}

}  // namespace scql::engine::util
