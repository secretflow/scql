// Copyright 2024 Ant Group Co., Ltd.
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

#include "engine/operator/bucket.h"

#include <filesystem>

#include "absl/strings/escaping.h"
#include "arrow/array/concatenate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/result.h"
#include "spdlog/spdlog.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/primitive_builder.h"
#include "engine/core/tensor_batch_reader.h"
#include "engine/core/type.h"
#include "engine/util/disk/arrow_writer.h"
#include "engine/util/filepath_helper.h"
#include "engine/util/ndarray_to_arrow.h"
#include "engine/util/psi_helper.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string Bucket::kOpType("Bucket");

const std::string& Bucket::Type() const { return kOpType; }

void Bucket::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  const auto& outputs = ctx->GetOutput(kOut);
  const auto& join_keys = ctx->GetInput(kJoinKey);

  YACL_ENFORCE(
      inputs.size() == outputs.size() && inputs.size() > 0,
      "Bucket input {} and output {} should have the same size and size > 0",
      kIn, kOut);
  YACL_ENFORCE(util::AreTensorsStatusMatched(
                   inputs, pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "Bucket input tensors' status should all be private");
  YACL_ENFORCE(util::AreTensorsStatusMatched(
                   join_keys, pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "Bucket join key tensors' status should all be private");
  YACL_ENFORCE(util::AreTensorsStatusMatched(
                   outputs, pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "Bucket output tensors' status should all be private");

  std::vector<std::string> input_party_codes =
      ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);
  YACL_ENFORCE(input_party_codes.size() == 2,
               "Join operator attribute {} must have exactly 2 elements",
               kInputPartyCodesAttr);
}

size_t GetMaxRowNum(ExecContext* ctx, size_t local_row_num,
                    std::vector<std::string> input_party_codes) {
  auto tag = ctx->GetNodeName() + "-ExchangeRowNum";
  auto link = ctx->GetSession()->GetLink();
  if (link->WorldSize() > 2) {
    link = link->SubWorld(tag, input_party_codes);
  }
  auto length_bufs = yacl::link::AllGather(
      link, yacl::ByteContainerView(&local_row_num, sizeof(size_t)), tag);
  size_t max_length = 0;
  for (const auto& o : length_bufs) {
    max_length = std::max(max_length, o.data<size_t>()[0]);
  }
  return max_length;
}

// return false, if no more batch to read
bool ReadArrays(std::vector<std::shared_ptr<TensorBatchReader>>& readers,
                arrow::ChunkedArrayVector* arrays) {
  for (const auto& reader : readers) {
    auto data = reader->Next();
    if (data == nullptr) {
      return false;
    }
    arrays->push_back(data);
  }
  return true;
}

void WriteArrays(
    std::vector<std::vector<std::shared_ptr<util::disk::ArrowWriter>>>& writers,
    std::vector<arrow::ChunkedArrayVector>& arrays, size_t bucket_num) {
  for (size_t i = 0; i < writers.size(); i++) {
    yacl::parallel_for(0, bucket_num, [&, i](int64_t begin, int64_t end) {
      for (int64_t j = begin; j < end; j++) {
        writers[i][j]->WriteBatch(*arrays[i][j]);
        // delete immediately to save memory
        arrays[i][j].reset();
      }
    });
  }
}

std::vector<arrow::ChunkedArrayVector> HashBucket(
    const arrow::ChunkedArrayVector& batch_arrays,
    const std::vector<int>& key_pos,
    const scql::engine::RepeatedPbTensor& join_key_pbs,
    const scql::engine::RepeatedPbTensor& output_pbs, size_t bucket_num) {
  std::vector<std::shared_ptr<arrow::ChunkedArray>> indices(bucket_num);
  {
    std::vector<UInt64TensorBuilder> result_builder(bucket_num);
    {
      std::vector<std::string> keys;
      {
        keys = util::Stringify(batch_arrays[key_pos[0]]);
        for (int i = 1; i < join_key_pbs.size(); ++i) {
          auto another_keys = util::Stringify(batch_arrays[key_pos[i]]);
          YACL_ENFORCE(keys.size() == another_keys.size(),
                       "tensor #{} batch size not equals with previous", i);

          keys = util::Combine(keys, another_keys);
        }
      }
      for (size_t i = 0; i < keys.size(); i++) {
        auto base64 = absl::Base64Escape(keys[i]);
        result_builder[std::hash<std::string>()(base64) % bucket_num].Append(i);
      }
    }
    for (size_t i = 0; i < bucket_num; i++) {
      TensorPtr indice;
      result_builder[i].Finish(&indice);
      indices[i] = indice->ToArrowChunkedArray();
    }
  }
  std::vector<arrow::ChunkedArrayVector> result(output_pbs.size());
  for (int i = 0; i < output_pbs.size(); i++) {
    // when calling function take, it will concatenate input array.
    // to avoid duplicated concatenating chunk, concatenate it when reading
    std::shared_ptr<arrow::Array> current_chunk =
        arrow::Concatenate(batch_arrays[i]->chunks()).ValueOrDie();
    auto arr = arrow::ChunkedArray::Make(
                   std::vector<std::shared_ptr<arrow::Array>>{current_chunk})
                   .ValueOrDie();
    arrow::ChunkedArrayVector arrays(bucket_num);
    for (size_t j = 0; j < bucket_num; j++) {
      arrow::Result<arrow::Datum> tmp;
      // delegate to apache arrow's take function
      tmp = arrow::compute::CallFunction("take", {arr, indices[j]});
      YACL_ENFORCE(tmp.ok(),
                   "caught error while invoking arrow take function:{}",
                   tmp.status().ToString());
      arrays[j] = tmp.ValueOrDie().chunked_array();
    }
    result[i] = std::move(arrays);
  }
  return result;
}

void Bucket::Execute(ExecContext* ctx) {
  const auto& input_pbs = ctx->GetInput(kIn);
  const auto& join_key_pbs = ctx->GetInput(kJoinKey);
  const auto& output_pbs = ctx->GetOutput(kOut);
  auto streaming_options = ctx->GetSession()->GetStreamingOptions();
  std::vector<TensorPtr> input_tensors;
  size_t length = 0;
  for (int i = 0; i < input_pbs.size(); i++) {
    auto tensor = ctx->GetTensorTable()->GetTensor(input_pbs[i].name());
    if (i == 0) {
      length = tensor->Length();
    }
    input_tensors.push_back(tensor);
  }
  std::vector<std::string> input_party_codes =
      ctx->GetStringValuesFromAttribute(kInputPartyCodesAttr);
  auto max_length = GetMaxRowNum(ctx, length, input_party_codes);
  SPDLOG_INFO("max bucket input size: {}", max_length);
  // bucket runs for scenes length is large than FLAGS_batch_row_num
  if (max_length <= streaming_options.streaming_row_num_threshold) {
    for (int i = 0; i < input_pbs.size(); i++) {
      ctx->GetTensorTable()->AddTensor(output_pbs[i].name(), input_tensors[i]);
    }
    return;
  }
  // create readers
  std::vector<std::shared_ptr<TensorBatchReader>> readers;
  for (int i = 0; i < input_pbs.size(); i++) {
    readers.push_back(input_tensors[i]->CreateBatchReader(
        streaming_options.batch_row_num / kBatchParallelism));
  }
  // create writer
  std::vector<std::vector<std::shared_ptr<util::disk::ArrowWriter>>>
      out_writers;
  auto bucket_num = (max_length + streaming_options.batch_row_num - 1) /
                    streaming_options.batch_row_num;

  SPDLOG_INFO("bucket num: {}", bucket_num);
  for (int i = 0; i < output_pbs.size(); i++) {
    std::vector<std::shared_ptr<util::disk::ArrowWriter>> arrow_writers(
        bucket_num);
    std::filesystem::path out_dir = util::CreateDirWithRandSuffix(
        streaming_options.dump_file_dir, output_pbs[i].name());
    for (size_t j = 0; j < bucket_num; j++) {
      arrow_writers[j] = std::make_shared<util::disk::ArrowWriter>(
          output_pbs[i].name(), ToArrowDataType(output_pbs[i].elem_type()),
          out_dir / std::to_string(j));
    }
    out_writers.push_back(std::move(arrow_writers));
  }
  std::vector<int> key_pos;
  for (int i = 0; i < join_key_pbs.size(); i++) {
    bool found = false;
    for (int j = 0; j < input_pbs.size(); j++) {
      if (join_key_pbs[i].name() == input_pbs[j].name()) {
        key_pos.emplace_back(j);
        found = true;
        break;
      }
    }
    YACL_ENFORCE(found, "join key {} not found in input_pbs",
                 join_key_pbs[i].name());
  }

  std::vector<arrow::ChunkedArrayVector> input_arrays;
  std::vector<std::vector<arrow::ChunkedArrayVector>> output_arrays;
  std::mutex read_mtx;
  std::mutex write_mtx;
  std::condition_variable read_cv;
  std::condition_variable write_cv;
  bool read_completed = false;
  bool bucket_completed = false;
  auto cal_future = std::async(std::launch::async, [&] {
    while (true) {
      std::vector<arrow::ChunkedArrayVector> batch_arrays;
      {
        std::unique_lock lock(read_mtx);
        if (read_completed && input_arrays.empty()) {
          bucket_completed = true;
          write_cv.notify_all();
          break;
        }
        if (!read_completed) {
          read_cv.wait(lock,
                       [&] { return !input_arrays.empty() || read_completed; });
        }
        if (input_arrays.empty()) {
          continue;
        }
        std::swap(input_arrays, batch_arrays);
      }
      read_cv.notify_all();
      SPDLOG_INFO("hash parallel size {}", batch_arrays.size());
      yacl::parallel_for(
          0, batch_arrays.size(), [&](int64_t begin, int64_t end) {
            for (int64_t j = begin; j < end; j++) {
              auto out_arrays =
                  HashBucket(batch_arrays[j], key_pos, join_key_pbs, output_pbs,
                             bucket_num);
              {
                std::lock_guard lock(write_mtx);
                output_arrays.push_back(out_arrays);
              }
              write_cv.notify_all();
            }
          });
    }
  });

  auto write_future = std::async(std::launch::async, [&] {
    while (true) {
      std::vector<std::vector<arrow::ChunkedArrayVector>> batch_arrays;
      {
        std::unique_lock<std::mutex> lock(write_mtx);
        if (bucket_completed && output_arrays.empty()) {
          break;
        }
        if (!bucket_completed) {
          write_cv.wait(
              lock, [&] { return !output_arrays.empty() || bucket_completed; });
        }
        if (output_arrays.empty()) {
          continue;
        }
        std::swap(output_arrays, batch_arrays);
      }
      for (auto& batch_array : batch_arrays) {
        WriteArrays(out_writers, batch_array, bucket_num);
      }
    }
  });

  int i = 0;
  try {
    while (true) {
      arrow::ChunkedArrayVector batch_arrays;
      if (!ReadArrays(readers, &batch_arrays)) {
        {
          std::unique_lock<std::mutex> lock(read_mtx);
          read_completed = true;
        }
        read_cv.notify_all();
        break;
      }
      SPDLOG_INFO("Read batch {}", i);
      i++;
      {
        std::unique_lock<std::mutex> lock(read_mtx);
        read_cv.wait(lock,
                     [&] { return input_arrays.size() < kBatchParallelism; });
        input_arrays.push_back(std::move(batch_arrays));
      }
      read_cv.notify_all();
    }
    cal_future.get();
  } catch (const std::exception& e) {
    SPDLOG_ERROR("read/cal error: {}", e.what());
    {
      std::unique_lock<std::mutex> lock(read_mtx);
      read_completed = true;
    }
    {
      std::unique_lock<std::mutex> lock(write_mtx);
      bucket_completed = true;
    }
    read_cv.notify_all();
    write_cv.notify_all();
    throw e;
  }
  write_future.get();
  for (int i = 0; i < output_pbs.size(); i++) {
    std::vector<FileArray> file_arrays(bucket_num);
    for (size_t j = 0; j < bucket_num; j++) {
      file_arrays[j].file_path = out_writers[i][j]->GetFilePath();
      file_arrays[j].len = out_writers[i][j]->GetRowNum();
      file_arrays[j].null_count = out_writers[i][j]->GetNullCount();
    }
    auto type = out_writers[i][0]->GetSchema()->field(0)->type();
    // remove writers to call ipc writer close()
    out_writers[i].clear();
    auto tensor = std::make_shared<DiskTensor>(file_arrays,
                                               output_pbs[i].elem_type(), type);
    tensor->SetAsBucketTensor();
    ctx->GetTensorTable()->AddTensor(output_pbs[i].name(), tensor);
  }
}

}  // namespace scql::engine::op