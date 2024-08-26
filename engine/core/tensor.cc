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

#include "engine/core/tensor.h"

#include "arrow/compute/cast.h"
#include "yacl/base/exception.h"

#include "engine/core/tensor_batch_reader.h"
#include "engine/core/type.h"
#include "engine/util/disk/arrow_reader.h"
#include "engine/util/disk/arrow_writer.h"

namespace scql::engine {

std::shared_ptr<arrow::ChunkedArray> ConvertArrayTo(
    const std::shared_ptr<arrow::ChunkedArray>& in_arr,
    const std::shared_ptr<arrow::DataType>& to_type) {
  arrow::compute::CastOptions options;
  options.allow_decimal_truncate = true;
  auto result = arrow::compute::Cast(in_arr, to_type, options);
  YACL_ENFORCE(result.ok(), "caught error while cast {} to {}: {}",
               in_arr->type()->ToString(), to_type->ToString(),
               result.status().ToString());
  return result.ValueOrDie().chunked_array();
}

std::shared_ptr<arrow::DataType> GetExpectedConvertType(
    const std::shared_ptr<arrow::DataType>& in_type) {
  // try to cast decimal128(x, 0) to int64
  if (in_type->id() == arrow::Type::DECIMAL128) {
    auto decimal_type =
        std::dynamic_pointer_cast<arrow::Decimal128Type>(in_type);
    if (decimal_type->scale() == 0) {
      return arrow::int64();
    }
  }
  return in_type;
}

MemTensor::MemTensor(std::shared_ptr<arrow::ChunkedArray> chunked_arr)
    : Tensor(pb::PrimitiveDataType::PrimitiveDataType_UNDEFINED),
      chunked_arr_(std::move(chunked_arr)) {
  auto to_type = GetExpectedConvertType(chunked_arr_->type());
  if (to_type->id() != chunked_arr_->type()->id()) {
    chunked_arr_ = ConvertArrayTo(chunked_arr_, to_type);
  }
  dtype_ = FromArrowDataType(chunked_arr_->type());
  YACL_ENFORCE(dtype_ != pb::PrimitiveDataType::PrimitiveDataType_UNDEFINED,
               "unsupported arrow data type: {}",
               chunked_arr_->type()->ToString());
}

std::shared_ptr<TensorBatchReader> MemTensor::CreateBatchReader(
    size_t batch_size) {
  return std::make_shared<MemTensorBatchReader>(shared_from_this(), batch_size);
}

DiskTensor::DiskTensor(std::vector<FileArray> file_arrays,
                       scql::pb::PrimitiveDataType dtype,
                       std::shared_ptr<arrow::DataType> arrow_type)
    : Tensor(dtype),
      file_arrays_(std::move(file_arrays)),
      arrow_type_(std::move(arrow_type)) {
  auto to_type = GetExpectedConvertType(arrow_type_);
  for (auto& file_array : file_arrays_) {
    len_ += file_array.len;
    null_count_ += file_array.null_count;
    if (to_type->id() != arrow_type_->id()) {
      util::disk::FileBatchReader reader(file_array.file_path);
      std::string new_file_name = file_array.file_path.string() + "-converted";
      util::disk::ArrowWriter writer("mock_name", to_type, new_file_name);
      constexpr size_t batch_size = 1000 * 1000;
      while (true) {
        auto batch = reader.ReadNext(batch_size);
        if (batch == nullptr) {
          break;
        }
        auto tmp = ConvertArrayTo(batch, to_type);
        YACL_ENFORCE(tmp->type()->ToString() == to_type->ToString());
        writer.WriteBatch(*tmp);
      }
      std::error_code ec;
      std::filesystem::remove(file_array.file_path, ec);
      if (ec.value() != 0) {
        SPDLOG_WARN("can not remove tmp dir: {}, msg: {}",
                    file_array.file_path.string(), ec.message());
      }
      file_array.file_path = new_file_name;
    }
  }
  if (to_type->id() != arrow_type_->id()) {
    arrow_type_ = to_type;
  }
}

std::shared_ptr<TensorBatchReader> DiskTensor::CreateBatchReader(
    size_t batch_size) {
  return std::make_shared<DiskTensorBatchReader>(shared_from_this(),
                                                 batch_size);
}

std::shared_ptr<arrow::ChunkedArray> DiskTensor::ToArrowChunkedArray() const {
  std::vector<std::shared_ptr<arrow::Array>> result_arrays;
  for (size_t i = 0; i < GetFileNum(); i++) {
    auto chunked_array = util::disk::ReadFileArray(GetFileArray(i).file_path);
    if (chunked_array == nullptr) {
      continue;
    }
    result_arrays.insert(result_arrays.end(), chunked_array->chunks().begin(),
                         chunked_array->chunks().end());
  }
  return std::make_shared<arrow::ChunkedArray>(result_arrays, arrow_type_);
}
}  // namespace scql::engine