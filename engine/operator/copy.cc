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

#include "engine/operator/copy.h"

#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/table.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor_constructor.h"
#include "engine/core/type.h"
#include "engine/util/logging.h"
#include "engine/util/ndarray_to_arrow.h"
#include "engine/util/spu_io.h"
#include "engine/util/table_util.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

const std::string Copy::kOpType("Copy");

const std::string& Copy::Type() const { return kOpType; }

void Copy::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  const auto& outputs = ctx->GetOutput(kOut);

  YACL_ENFORCE(inputs.size() == outputs.size(),
               "Copy input {} and output {} should have the same size", kIn,
               kOut);
  YACL_ENFORCE(util::AreTensorsStatusMatched(
                   inputs, pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "Copy input tensors' status should all be private");
  YACL_ENFORCE(util::AreTensorsStatusMatched(
                   outputs, pb::TensorStatus::TENSORSTATUS_PRIVATE),
               "Copy output tensors' status should all be private");
}

void Copy::Execute(ExecContext* ctx) {
  auto logger = ctx->GetActiveLogger();
  const auto& input_pbs = ctx->GetInput(kIn);
  const auto& output_pbs = ctx->GetOutput(kOut);
  const std::string& from_party =
      ctx->GetStringValueFromAttribute(kInputPartyCodesAttr);
  const std::string& to_party =
      ctx->GetStringValueFromAttribute(kOutputPartyCodesAttr);
  const std::string self_party = ctx->GetSession()->SelfPartyCode();

  auto lctx = ctx->GetSession()->GetLink();

  if (self_party == from_party) {
    auto table = util::ConstructTableFromTensors(ctx, input_pbs);
    YACL_ENFORCE(table, "construct table failed");

    auto send_to_rank = ctx->GetSession()->GetPartyRank(to_party);
    YACL_ENFORCE(send_to_rank != -1, "unknown rank for party={}", to_party);

    auto reader = arrow::TableBatchReader(table);
    reader.set_chunksize(batch_size_);

    std::shared_ptr<arrow::RecordBatch> batch;
    int count = 0;
    int64_t total = 0;
    while (reader.ReadNext(&batch).ok() && batch != nullptr) {
      count++;
      size_t current_rows = batch->num_rows();
      total += current_rows;

      auto buffer = SerializeRecordBatch(std::move(batch));
      YACL_ENFORCE(buffer, "serialize record batch failed");

      lctx->Send(
          send_to_rank,
          yacl::ByteContainerView(buffer->mutable_data(), buffer->size()),
          ctx->GetNodeName());

      SPDLOG_LOGGER_INFO(logger, "sent {} rows in batch {}, {} rows in total.",
                         current_rows, count, total);
    }

    // append a dummy batch to the tail of the batch to indicate
    // the batching is done
    {
      std::shared_ptr<arrow::RecordBatch> dummy_batch;
      ASSIGN_OR_THROW_ARROW_STATUS(
          dummy_batch, arrow::RecordBatch::MakeEmpty(table->schema()));
      auto buffer = SerializeRecordBatch(std::move(dummy_batch));
      YACL_ENFORCE(buffer, "serialize record batch failed");

      lctx->Send(
          send_to_rank,
          yacl::ByteContainerView(buffer->mutable_data(), buffer->size()),
          ctx->GetNodeName());
    }
  } else {
    auto recv_from_rank = ctx->GetSession()->GetPartyRank(from_party);
    YACL_ENFORCE(recv_from_rank != -1, "unknown rank for party={}", from_party);
    std::shared_ptr<arrow::Table> merged_table;
    int count = 0;
    int64_t total = 0;
    while (true) {
      yacl::Buffer value = lctx->Recv(recv_from_rank, ctx->GetNodeName());
      std::shared_ptr<arrow::Table> table = DeserializeTable(std::move(value));
      YACL_ENFORCE(table, "deserialize table failed");

      auto current_rows = table->num_rows();
      total += current_rows;

      if (!merged_table) {
        merged_table = table;
      } else {
        auto result = arrow::ConcatenateTables({merged_table, table});
        YACL_ENFORCE(result.ok(), "concatenate table failed");
        merged_table = result.ValueOrDie();
      }

      if (table->num_rows() == 0) {
        break;
      }

      count++;
      SPDLOG_LOGGER_INFO(logger,
                         "received {} rows in batch {}, {} rows in total.",
                         current_rows, count, total);
    }

    InsertTensorsFromTable(ctx, output_pbs, std::move(merged_table));
  }
}

std::shared_ptr<arrow::Buffer> Copy::SerializeRecordBatch(
    std::shared_ptr<arrow::RecordBatch> batch) {
  std::shared_ptr<arrow::io::BufferOutputStream> output_stream;
  ASSIGN_OR_THROW_ARROW_STATUS(output_stream,
                               arrow::io::BufferOutputStream::Create());

  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  ASSIGN_OR_THROW_ARROW_STATUS(
      writer, arrow::ipc::MakeStreamWriter(output_stream, batch->schema()));

  THROW_IF_ARROW_NOT_OK(writer->WriteRecordBatch(*batch));
  THROW_IF_ARROW_NOT_OK(writer->Close());

  std::shared_ptr<arrow::Buffer> buffer;
  ASSIGN_OR_THROW_ARROW_STATUS(buffer, output_stream->Finish());

  return buffer;
}

std::shared_ptr<arrow::Table> Copy::DeserializeTable(yacl::Buffer value) {
  auto buffer = std::make_shared<arrow::Buffer>(value.data<const uint8_t>(),
                                                value.size());
  value.release();  // buffer owns datas.

  auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);

  std::shared_ptr<arrow::ipc::RecordBatchStreamReader> reader;
  ASSIGN_OR_THROW_ARROW_STATUS(
      reader, arrow::ipc::RecordBatchStreamReader::Open(buffer_reader));

  std::shared_ptr<arrow::Table> table;
  ASSIGN_OR_THROW_ARROW_STATUS(table, reader->ToTable());
  THROW_IF_ARROW_NOT_OK(table->Validate());

  return table;
}

void Copy::InsertTensorsFromTable(ExecContext* ctx,
                                  const RepeatedPbTensor& output_pbs,
                                  std::shared_ptr<arrow::Table> table) {
  YACL_ENFORCE(table->num_columns() == output_pbs.size(),
               "receive table column size={} not equal to output size={}",
               table->num_columns(), output_pbs.size());
  for (int i = 0; i < output_pbs.size(); ++i) {
    auto chunked_arr = table->column(i);
    YACL_ENFORCE(chunked_arr, "get column(idx={}) from table failed", i);
    auto tensor = TensorFrom(std::move(chunked_arr));

    ctx->GetTensorTable()->AddTensor(output_pbs[i].name(), tensor);
  }
}

}  // namespace scql::engine::op