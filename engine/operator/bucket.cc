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

#include <atomic>
#include <boost/container_hash/hash.hpp>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <future>
#include <memory>
#include <optional>
#include <queue>
#include <string>

#include "arrow/array/concatenate.h"
#include "arrow/compute/exec.h"
#include "arrow/result.h"
#include "spdlog/spdlog.h"

#include "engine/core/primitive_builder.h"
#include "engine/core/tensor.h"
#include "engine/core/tensor_batch_reader.h"
#include "engine/core/tensor_constructor.h"
#include "engine/core/type.h"
#include "engine/util/concurrent_queue.h"
#include "engine/util/disk/arrow_writer.h"
#include "engine/util/filepath_helper.h"
#include "engine/util/psi/batch_provider.h"
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
                    const std::vector<std::string>& input_party_codes) {
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
                arrow::ArrayVector* arrays) {
  arrow::ChunkedArrayVector read_datas;
  for (const auto& reader : readers) {
    auto data = reader->Next();
    if (data == nullptr) {
      return false;
    }
    read_datas.push_back(data);
  }
  for (auto& read_data : read_datas) {
    arrays->push_back(arrow::Concatenate(read_data->chunks()).ValueOrDie());
  }
  return true;
}

void WriteArrays(std::vector<std::shared_ptr<BucketTensorConstructor>>& writers,
                 std::vector<arrow::ChunkedArrayVector>& arrays,
                 size_t bucket_num) {
  for (size_t i = 0; i < writers.size(); i++) {
    for (size_t j = 0; j < bucket_num; j++) {
      writers[i]->InsertBucket(arrays[i][j], j);
      // delete immediately to save memory
      arrays[i][j].reset();
    }
  }
}

std::vector<arrow::ChunkedArrayVector> FilterByIndex(
    const arrow::ArrayVector& arrays, const arrow::ChunkedArrayVector& indices,
    const scql::engine::RepeatedPbTensor& output_pbs, size_t bucket_num) {
  std::vector<arrow::ChunkedArrayVector> result(output_pbs.size());
  for (int i = 0; i < output_pbs.size(); i++) {
    // when calling function take, it will concatenate input array.
    // to avoid duplicated concatenating chunk, concatenate it when reading
    auto arr = arrow::ChunkedArray::Make({arrays[i]}).ValueOrDie();
    arrow::ChunkedArrayVector arrays(bucket_num);
    yacl::parallel_for(0, bucket_num, [&](int64_t begin, int64_t end) {
      for (int64_t j = begin; j < end; j++) {
        arrow::Result<arrow::Datum> tmp;
        // delegate to apache arrow's take function
        tmp = arrow::compute::CallFunction("take", {arr, indices[j]});
        YACL_ENFORCE(tmp.ok(),
                     "caught error while invoking arrow take function:{}",
                     tmp.status().ToString());
        arrays[j] = tmp.ValueOrDie().chunked_array();
      }
    });
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
  std::vector<std::shared_ptr<TensorBatchReader>> readers(input_pbs.size());
  for (int i = 0; i < input_pbs.size(); i++) {
    readers[i] = (input_tensors[i]->CreateBatchReader(
        streaming_options.batch_row_num / kBatchParallelism));
  }
  // create writer
  std::vector<std::shared_ptr<BucketTensorConstructor>> tensor_constructors(
      output_pbs.size());
  auto bucket_num = (max_length + streaming_options.batch_row_num - 1) /
                    streaming_options.batch_row_num;

  SPDLOG_INFO("bucket num: {}", bucket_num);
  for (int i = 0; i < output_pbs.size(); i++) {
    std::filesystem::path out_dir = util::CreateDirWithRandSuffix(
        streaming_options.dump_file_dir, output_pbs[i].name());
    tensor_constructors[i] = std::make_shared<DiskBucketTensorConstructor>(
        output_pbs[i].name(), ToArrowDataType(output_pbs[i].elem_type()),
        output_pbs[i].elem_type(), out_dir, bucket_num);
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

  auto cal_future = std::async(std::launch::async, [&] {
    std::vector<std::future<void>> futures;
    int i = 0;
    auto read_result = read_queue_.Pop();
    while (read_result.has_value()) {
      SPDLOG_INFO("Take batch: {}", i);
      auto out_arrays =
          FilterByIndex(read_result.value().data_arrays,
                        read_result.value().indice, output_pbs, bucket_num);
      write_queue_.Push(out_arrays);
      i++;
      read_result = read_queue_.Pop();
    }
    write_queue_.Close();
  });

  auto write_future = std::async(std::launch::async, [&] {
    int i = 0;
    auto data = write_queue_.Pop();
    while (data.has_value()) {
      SPDLOG_INFO("Write batch: {}", i);
      WriteArrays(tensor_constructors, data.value(), bucket_num);
      i++;
      data = write_queue_.Pop();
    }
  });

  try {
    std::queue<std::future<void>> futures;
    int num = 0;
    while (true) {
      arrow::ArrayVector batch_arrays;
      if (!ReadArrays(readers, &batch_arrays)) {
        break;
      }
      SPDLOG_INFO("Read batch {}", num);
      // parallel calculate filter indices
      auto f = std::async(
          std::launch::async,
          [&](arrow::ArrayVector&& batch_arrays) {
            arrow::ChunkedArrayVector indices(bucket_num);
            std::vector<UInt64TensorBuilder> result_builder(indices.size());
            {
              std::vector<std::string> keys;
              {
                keys = util::Stringify(batch_arrays[key_pos[0]]);
                for (size_t i = 1; i < key_pos.size(); ++i) {
                  auto another_keys = util::Stringify(batch_arrays[key_pos[i]]);
                  YACL_ENFORCE(keys.size() == another_keys.size(),
                               "tensor #{} batch size not equals with previous",
                               i);

                  keys = util::Combine(keys, another_keys);
                }
              }
              boost::hash<std::string> hasher;
              for (size_t i = 0; i < keys.size(); i++) {
                result_builder[hasher(keys[i]) % indices.size()].Append(i);
              }
            }
            for (size_t i = 0; i < indices.size(); i++) {
              TensorPtr indice;
              result_builder[i].Finish(&indice);
              indices[i] = indice->ToArrowChunkedArray();
            }
            read_queue_.Push({batch_arrays, indices});
          },
          std::move(batch_arrays));
      futures.push(std::move(f));
      if (futures.size() >= kBatchParallelism) {
        futures.front().get();
        futures.pop();
      }
      num++;
    }
    while (!futures.empty()) {
      futures.front().get();
      futures.pop();
    }
    // close read queue
    read_queue_.Close();
    cal_future.get();
  } catch (const std::exception& e) {
    SPDLOG_ERROR("read/cal error: {}", e.what());
    // close read queue
    read_queue_.Close();
    throw e;
  }
  write_future.get();
  for (int i = 0; i < output_pbs.size(); i++) {
    TensorPtr tensor;
    tensor_constructors[i]->Finish(&tensor);
    ctx->GetTensorTable()->AddTensor(output_pbs[i].name(), tensor);
  }
}

}  // namespace scql::engine::op
