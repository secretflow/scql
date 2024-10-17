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

#include "arrow/array/concatenate.h"
#include "arrow/compute/cast.h"
#include "arrow/csv/writer.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/io/file.h"
#include "arrow/ipc/writer.h"
#include "arrow/table.h"
#include "gflags/gflags.h"

#include "engine/audit/audit_log.h"
#include "engine/core/arrow_helper.h"
#include "engine/core/tensor_batch_reader.h"
#include "engine/util/filepath_helper.h"
#include "engine/util/tensor_util.h"
#include "engine/util/time_util.h"
namespace scql::engine::op {

// TODO(jingshi) : temporary add flags here to keep the simplicity of op's
// initialization, modify it later.(maybe add options in session)
DEFINE_bool(enable_restricted_write_path, true,
            "whether restrict path for file to write");
DEFINE_string(
    restricted_write_path, "./data",
    "in where the file is allowed to write if enable restricted write path");
DEFINE_string(null_string_to_write, "NULL",
              "the string to write for null values");
// TODO: 1)work with kuscia, 2)support set in ENV
DEFINE_string(output_s3_endpoint, "", "the endpoint of output s3/minio/oss");
DEFINE_string(output_s3_access_key, "",
              "the access key id of output s3/minio/oss");
DEFINE_string(output_s3_secret_key, "",
              "the secret access key of output s3/minio/oss");
DEFINE_bool(output_s3_enalbe_ssl, true,
            "default enable ssl, if s3 server not enable ssl, set to false");
DEFINE_string(output_s3_ca_dir_path, "/etc/ssl/certs/",
              "directory where the certificates stored to verify s3 server");
DEFINE_bool(
    output_s3_force_virtual_addressing, true,
    "default set to true to work with oss, for minio please set to false");

const std::string DumpFile::kOpType("DumpFile");
const std::string& DumpFile::Type() const { return kOpType; }

namespace {

void InitS3Once() {
  static std::once_flag flag;
  std::call_once(flag, []() {
    arrow::fs::FileSystemGlobalOptions global_options;
    global_options.tls_ca_dir_path = FLAGS_output_s3_ca_dir_path;
    THROW_IF_ARROW_NOT_OK(arrow::fs::Initialize(global_options));

    arrow::fs::S3GlobalOptions output_s3_options;
    output_s3_options.log_level = arrow::fs::S3LogLevel::Warn;
    THROW_IF_ARROW_NOT_OK(arrow::fs::InitializeS3(output_s3_options));
  });
}

std::shared_ptr<arrow::io::OutputStream> BuildStreamFromS3(
    const std::string& prefix, const std::string& path_without_prefix) {
  InitS3Once();
  arrow::fs::S3Options options;
  std::string s3_endpoint = FLAGS_output_s3_endpoint;
  bool use_ssl = util::GetAndRemoveS3EndpointPrefix(s3_endpoint);
  options.endpoint_override = s3_endpoint;
  options.force_virtual_addressing = FLAGS_output_s3_force_virtual_addressing;
  options.ConfigureAccessKey(FLAGS_output_s3_access_key,
                             FLAGS_output_s3_secret_key);
  if (!use_ssl || !FLAGS_output_s3_enalbe_ssl) {
    options.scheme = "http";
  }
  std::shared_ptr<arrow::fs::S3FileSystem> fs;
  ASSIGN_OR_THROW_ARROW_STATUS(fs, arrow::fs::S3FileSystem::Make(options));

  arrow::fs::FileInfo info;
  ASSIGN_OR_THROW_ARROW_STATUS(info, fs->GetFileInfo(path_without_prefix));
  YACL_ENFORCE(info.type() == arrow::fs::FileType::NotFound,
               "s3 file={}{} exists before write", prefix, path_without_prefix);

  std::shared_ptr<arrow::io::OutputStream> out_stream;
  ASSIGN_OR_THROW_ARROW_STATUS(out_stream,
                               fs->OpenOutputStream(path_without_prefix));
  return out_stream;
}

std::shared_ptr<arrow::io::OutputStream> BuildOutputStream(
    const std::string& in_filepath, bool is_restricted,
    const std::string& restricted_filepath) {
  auto prefix = util::GetS3LikeScheme(in_filepath);
  auto filepath_without_prefix = in_filepath.substr(prefix.length());
  if (!prefix.empty()) {
    // s3 file
    util::CheckS3LikeUrl(filepath_without_prefix, is_restricted,
                         restricted_filepath);
    return BuildStreamFromS3(prefix, filepath_without_prefix);
  }
  // local file
  auto absolute_path_file = util::CheckAndGetAbsoluteLocalPath(
      in_filepath, is_restricted, restricted_filepath);
  YACL_ENFORCE(!std::filesystem::exists(absolute_path_file),
               "file={} exists before write", absolute_path_file);
  std::filesystem::create_directories(
      std::filesystem::path(absolute_path_file).parent_path());
  std::shared_ptr<arrow::io::OutputStream> out_stream;
  ASSIGN_OR_THROW_ARROW_STATUS(
      out_stream, arrow::io::FileOutputStream::Open(absolute_path_file, false));
  return out_stream;
}

void WriteTensors(
    const std::shared_ptr<arrow::Schema>& schema,
    const arrow::csv::WriteOptions& options,
    const std::shared_ptr<arrow::io::OutputStream>& out_stream,
    const std::vector<std::shared_ptr<TensorBatchReader>>& readers) {
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  ASSIGN_OR_THROW_ARROW_STATUS(
      writer, arrow::csv::MakeCSVWriter(out_stream, schema, options));
  while (true) {
    arrow::ArrayVector arrays;
    bool read_to_end = false;
    for (size_t i = 0; i < readers.size(); ++i) {
      auto chunked_arr = readers[i]->Next();
      if (chunked_arr == nullptr) {
        read_to_end = true;
        break;
      }
      if (arrow::is_temporal(schema->field(i)->type()->id())) {
        auto result =
            arrow::compute::Cast(chunked_arr, schema->field(i)->type());
        YACL_ENFORCE(result.ok(), "caught error while cast to {}: {}",
                     schema->field(i)->type()->ToString(),
                     result.status().ToString());
        chunked_arr = result.ValueOrDie().chunked_array();
      }
      arrays.emplace_back(
          arrow::Concatenate(chunked_arr->chunks()).ValueOrDie());
    }
    if (read_to_end) {
      break;
    }
    int64_t length = arrays[0]->length();
    auto batch = arrow::RecordBatch::Make(schema, length, arrays);
    THROW_IF_ARROW_NOT_OK(writer->WriteRecordBatch(*batch));
  }
  THROW_IF_ARROW_NOT_OK(writer->Close());
}

}  // namespace

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

  // 1.construct data to table
  std::vector<std::shared_ptr<arrow::Field>> fields;
  const auto start_time = std::chrono::system_clock::now();
  int32_t batch_size = 1024 * 100;  // default 1024 is too small for large data
  std::vector<std::shared_ptr<TensorBatchReader>> readers;
  auto total_len = 0;
  for (int i = 0; i < input_pbs.size(); ++i) {
    const auto& input_pb = input_pbs[i];
    auto tensor = ctx->GetTensorTable()->GetTensor(input_pb.name());
    YACL_ENFORCE(tensor != nullptr, "get tensor={} from tensor table failed",
                 input_pb.name());
    if (input_pb.elem_type() == pb::PrimitiveDataType::TIMESTAMP &&
        !ctx->GetSession()->TimeZone().empty()) {
      // FIXME(jingshi): avoid memory cost when time_zone set
      tensor = util::CompensateTimeZone(tensor, ctx->GetSession()->TimeZone());
    }
    if (i == 0) {
      total_len = tensor->Length();
    } else {
      YACL_ENFORCE(tensor->Length() == total_len,
                   "input tensor length not equal");
    }
    auto reader = tensor->CreateBatchReader(batch_size);
    readers.push_back(reader);
    auto column_out = util::GetStringValue(output_pbs[i]);
    auto arrow_type = tensor->ArrowType();
    if (input_pb.elem_type() == pb::PrimitiveDataType::TIMESTAMP ||
        input_pb.elem_type() == pb::PrimitiveDataType::DATETIME) {
      arrow_type = arrow::timestamp(arrow::TimeUnit::SECOND);
    }
    fields.emplace_back(arrow::field(column_out, arrow_type));
  }

  // 2.write table to file
  arrow::csv::WriteOptions options;
  options.null_string = FLAGS_null_string_to_write;
  options.batch_size = batch_size;
  options.delimiter =
      ctx->GetStringValueFromAttribute(kFieldDeliminatorAttr).front();
  options.eol = ctx->GetStringValueFromAttribute(kLineTerminatorAttr);
  const auto quoting = ctx->GetInt64ValueFromAttribute(kQuotingStyleAttr);
  if (quoting == kQuotingNone) {
    options.quoting_style = arrow::csv::QuotingStyle::None;
  } else if (quoting == kQuotingNeeded) {
    options.quoting_style = arrow::csv::QuotingStyle::Needed;
  } else if (quoting == kQuotingAllValid) {
    options.quoting_style = arrow::csv::QuotingStyle::AllValid;
  } else {
    YACL_THROW("unsupported quoting style {}", quoting);
  }

  auto file_path = ctx->GetStringValueFromAttribute(kFilePathAttr);
  auto out_stream =
      BuildOutputStream(file_path, FLAGS_enable_restricted_write_path,
                        FLAGS_restricted_write_path);
  YACL_ENFORCE(out_stream, "create output stream failed");
  auto schema = arrow::schema(fields);
  WriteTensors(schema, options, out_stream, readers);
  ctx->GetSession()->SetAffectedRows(total_len);

  audit::RecordDumpFileNodeDetail(*ctx, file_path, start_time);
}
};  // namespace scql::engine::op