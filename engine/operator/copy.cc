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
#include "engine/core/type.h"
#include "engine/util/ndarray_to_arrow.h"
#include "engine/util/spu_io.h"
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
  const auto& input_pbs = ctx->GetInput(kIn);
  const auto& output_pbs = ctx->GetOutput(kOut);
  const std::string& from_party =
      ctx->GetStringValueFromAttribute(kInputPartyCodesAttr);
  const std::string& to_party =
      ctx->GetStringValueFromAttribute(kOutputPartyCodesAttr);
  const std::string self_party = ctx->GetSession()->SelfPartyCode();

  auto lctx = ctx->GetSession()->GetLink();

  if (self_party == from_party) {
    auto table = ConstructTableFromTensors(ctx, input_pbs);
    YACL_ENFORCE(table, "construct table failed");

    auto buffer = SerializeTable(std::move(table));
    YACL_ENFORCE(buffer, "serialize table failed");

    auto send_to_rank = ctx->GetSession()->GetPartyRank(to_party);
    YACL_ENFORCE(send_to_rank != -1, "unknown rank for party={}", to_party);
    lctx->Send(send_to_rank,
               yacl::ByteContainerView(buffer->mutable_data(), buffer->size()),
               ctx->GetNodeName());
  } else {
    auto recv_from_rank = ctx->GetSession()->GetPartyRank(from_party);
    YACL_ENFORCE(recv_from_rank != -1, "unknown rank for party={}", from_party);
    yacl::Buffer value = lctx->Recv(recv_from_rank, ctx->GetNodeName());

    auto table = DeserializeTable(std::move(value));
    YACL_ENFORCE(table, "deserialize table failed");

    InsertTensorsFromTable(ctx, output_pbs, std::move(table));
  }
}

std::shared_ptr<arrow::Table> Copy::ConstructTableFromTensors(
    ExecContext* ctx, const RepeatedTensor& input_pbs) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> chunked_arrs;
  int64_t pre_length = 0;
  for (int i = 0; i < input_pbs.size(); ++i) {
    const auto& input_pb = input_pbs[i];
    auto tensor = ctx->GetTensorTable()->GetTensor(input_pb.name());
    YACL_ENFORCE(tensor != nullptr, "get tensor={} from tensor table failed",
                 input_pb.name());
    auto chunked_arr = tensor->ToArrowChunkedArray();
    if (i > 0) {
      YACL_ENFORCE(chunked_arr->length() == pre_length,
                   "input tensors must have the same length");
    }
    pre_length = chunked_arr->length();

    fields.emplace_back(arrow::field(input_pb.name(), chunked_arr->type()));
    chunked_arrs.emplace_back(chunked_arr);
  }

  auto table = arrow::Table::Make(arrow::schema(fields), chunked_arrs);
  return table;
}

std::shared_ptr<arrow::Buffer> Copy::SerializeTable(
    std::shared_ptr<arrow::Table> table) {
  std::shared_ptr<arrow::io::BufferOutputStream> out_stream;
  ASSIGN_OR_THROW_ARROW_STATUS(out_stream,
                               arrow::io::BufferOutputStream::Create());

  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  ASSIGN_OR_THROW_ARROW_STATUS(
      writer, arrow::ipc::MakeStreamWriter(out_stream, table->schema()));

  THROW_IF_ARROW_NOT_OK(writer->WriteTable(*table));
  THROW_IF_ARROW_NOT_OK(writer->Close());

  std::shared_ptr<arrow::Buffer> buffer;
  ASSIGN_OR_THROW_ARROW_STATUS(buffer, out_stream->Finish());
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
                                  const RepeatedTensor& output_pbs,
                                  std::shared_ptr<arrow::Table> table) {
  YACL_ENFORCE(table->num_columns() == output_pbs.size(),
               "receive table column size={} not equal to output size={}",
               table->num_columns(), output_pbs.size());
  for (int i = 0; i < output_pbs.size(); ++i) {
    auto chunked_arr = table->column(i);
    YACL_ENFORCE(chunked_arr, "get column(idx={}) from table failed", i);
    auto tensor = std::make_shared<Tensor>(std::move(chunked_arr));

    ctx->GetTensorTable()->AddTensor(output_pbs[i].name(), tensor);
  }
}

}  // namespace scql::engine::op