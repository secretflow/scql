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

#include "engine/operator/dump_file.h"

#include <filesystem>

#include "arrow/csv/writer.h"
#include "arrow/io/file.h"
#include "arrow/table.h"
#include "gflags/gflags.h"

#include "engine/core/arrow_helper.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

// TODO(jingshi) : temporary add flags here to keep the simplicity of op's
// initialization, modify it later.(maybe add options in session)
DEFINE_bool(enable_restricted_write_path, true,
            "whether restrict path for file to write");
DEFINE_string(
    restricted_write_path, "./data",
    "in where the file is allow to write if enable restricted write path");

const std::string DumpFile::kOpType("DumpFile");
const std::string& DumpFile::Type() const { return kOpType; }

void DumpFile::Validate(ExecContext* ctx) {
  const auto& inputs = ctx->GetInput(kIn);
  YACL_ENFORCE(inputs.size() > 0, "DumpFile input size cannot be 0");
  const auto& outputs = ctx->GetOutput(kOut);
  YACL_ENFORCE(inputs.size() == outputs.size(),
               "DumpFile inputs' size={} and outputs' size={} not equal",
               inputs.size(), outputs.size());

  YACL_ENFORCE(util::AreTensorsStatusMatched(inputs, pb::TENSORSTATUS_PRIVATE),
               "DumpFile inputs' status are not all private");
}

void DumpFile::Execute(ExecContext* ctx) {
  const auto& input_pbs = ctx->GetInput(kIn);
  const auto& output_pbs = ctx->GetOutput(kOut);

  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> chunked_arrs;

  for (int i = 0; i < input_pbs.size(); ++i) {
    const auto& input_pb = input_pbs[i];
    auto tensor = ctx->GetTensorTable()->GetTensor(input_pb.name());
    YACL_ENFORCE(tensor != nullptr, "get tensor={} from tensor table failed",
                 input_pb.name());
    auto chunked_arr = tensor->ToArrowChunkedArray();

    auto column_out = util::GetStringValue(output_pbs[i]);
    fields.emplace_back(arrow::field(column_out, chunked_arr->type()));
    chunked_arrs.emplace_back(chunked_arr);
  }

  int64_t length = chunked_arrs[0]->length();
  for (size_t i = 1; i < chunked_arrs.size(); ++i) {
    YACL_ENFORCE(chunked_arrs[i]->length() == length,
                 "length of chunked_arr#{} not equal to the previous", i);
  }

  auto table = arrow::Table::Make(arrow::schema(fields), chunked_arrs);
  YACL_ENFORCE(table, "create table failed");

  const auto& absolute_path_file = GetAbsolutePathFile(ctx);
  YACL_ENFORCE(!std::filesystem::exists(absolute_path_file),
               "file={} exists before write", absolute_path_file);
  std::filesystem::create_directories(
      std::filesystem::path(absolute_path_file).parent_path());

  std::shared_ptr<arrow::io::FileOutputStream> out_stream;
  ASSIGN_OR_THROW_ARROW_STATUS(
      out_stream, arrow::io::FileOutputStream::Open(absolute_path_file, false));

  arrow::csv::WriteOptions options;
  options.delimiter =
      ctx->GetStringValueFromAttribute(kDeliminatorAttr).front();

  THROW_IF_ARROW_NOT_OK(
      arrow::csv::WriteCSV(*table, options, out_stream.get()));

  ctx->GetSession()->SetAffectedRows(length);
  // TODO(jingshi): support put file to oss/minio/s3
}

std::string DumpFile::GetAbsolutePathFile(ExecContext* ctx) {
  const std::string& file_path_attr =
      ctx->GetStringValueFromAttribute(kFilePathAttr);
  std::filesystem::path final_path;
  if (FLAGS_enable_restricted_write_path) {
    auto pos = file_path_attr.find("..");
    YACL_ENFORCE(
        pos == std::string::npos,
        "enable restricted write path, path={} can not contain '..' for safety",
        file_path_attr);

    std::filesystem::path restricted_path(FLAGS_restricted_write_path);
    std::filesystem::path relative_path(file_path_attr);
    YACL_ENFORCE(
        relative_path.is_relative(),
        "enable restricted write path, path={} from SQL must be relative",
        file_path_attr);

    final_path = restricted_path / relative_path;
  } else {
    final_path = std::filesystem::path(file_path_attr);
  }

  if (!final_path.is_absolute()) {
    final_path = std::filesystem::current_path() / final_path;
  }
  return final_path.lexically_normal().string();
}

};  // namespace scql::engine::op