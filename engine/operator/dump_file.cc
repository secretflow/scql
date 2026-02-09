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

#include "absl/strings/match.h"
#include "arrow/array/concatenate.h"
#include "arrow/compute/cast.h"
#include "arrow/csv/writer.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/flight/client.h"
#include "arrow/io/file.h"
#include "arrow/ipc/writer.h"
#include "butil/base64.h"
#include "dataproxy_sdk/data_proxy_file.h"
#include "gflags/gflags.h"
#include "google/protobuf/util/json_util.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor_batch_reader.h"
#include "engine/exe/flags.h"
#include "engine/framework/exec.h"
#include "engine/util/datamesh_helper.h"
#include "engine/util/filepath_helper.h"
#include "engine/util/ssl_helper.h"
#include "engine/util/tensor_util.h"
#include "engine/util/time_util.h"
#include "engine/util/upload_info_helper.h"

#include "engine/datasource/dataproxy_conf.pb.h"
#include "engine/util/dp/flight.pb.h"
#include "google/protobuf/any.pb.h"

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
DEFINE_string(dataproxy_upload_info, "",
              "If not empty, the local temporary csv file will be "
              "uploaded via dataproxy according to the uplode info, which is "
              "used to set the domaindata");
DEFINE_string(
    output_dataproxy_json, "",
    "JSON-formatted DataProxy configuration for direct writing, e.g:"
    R"json('{"dp_uri":"grpc+tcp://host:port","datasource":{"type":"odps","info":{"odps":{"endpoint":"xxx","project":"xxx","accessKeyId":"xxx","accessKeySecret":"xxx"}}}}')json");

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
    const std::shared_ptr<spdlog::logger>& logger, const std::string& prefix,
    std::string path_without_prefix) {
  InitS3Once();
  arrow::fs::S3Options options;
  options.force_virtual_addressing = FLAGS_output_s3_force_virtual_addressing;
  options.ConfigureAccessKey(FLAGS_output_s3_access_key,
                             FLAGS_output_s3_secret_key);

  std::string s3_endpoint = FLAGS_output_s3_endpoint;
  bool use_ssl = util::GetAndRemoveS3EndpointPrefix(s3_endpoint);
  options.endpoint_override = s3_endpoint;
  if (!use_ssl || !FLAGS_output_s3_enalbe_ssl) {
    options.scheme = "http";
  }

  SPDLOG_LOGGER_INFO(
      logger, "s3_endpoint({}), scheme({}), path_without_prefix({})",
      options.endpoint_override, options.scheme, path_without_prefix);

  std::shared_ptr<arrow::fs::S3FileSystem> fs;
  ASSIGN_OR_THROW_ARROW_STATUS(fs, arrow::fs::S3FileSystem::Make(options));

  arrow::fs::FileInfo info;
  ASSIGN_OR_THROW_ARROW_STATUS(info, fs->GetFileInfo(path_without_prefix));
  YACL_ENFORCE(info.type() == arrow::fs::FileType::NotFound,
               "s3 file={}{} exists before write", prefix, path_without_prefix);

  std::shared_ptr<arrow::io::OutputStream> out_stream;
  ASSIGN_OR_THROW_ARROW_STATUS(out_stream,
                               fs->OpenOutputStream(path_without_prefix));

  SPDLOG_LOGGER_INFO(logger, "s3 output stream created");
  return out_stream;
}

std::shared_ptr<arrow::io::OutputStream> BuildOutputStream(
    const std::shared_ptr<spdlog::logger>& logger,
    const std::string& in_filepath, bool is_restricted,
    const std::string& restricted_filepath) {
  auto prefix = util::GetS3LikeScheme(in_filepath);
  auto filepath_without_prefix = in_filepath.substr(prefix.length());

  if (!prefix.empty()) {
    // s3 file
    util::CheckS3LikeUrl(filepath_without_prefix, is_restricted,
                         restricted_filepath);
    return BuildStreamFromS3(logger, prefix, filepath_without_prefix);
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

  SPDLOG_LOGGER_INFO(logger, "local output stream created, absolute path({})",
                     absolute_path_file);
  return out_stream;
}

// handles the generation of RecordBatches from TensorBatchReaders and processes
// them via callbacks
void ProcessRecordBatchesFromTensors(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::vector<std::shared_ptr<TensorBatchReader>>& readers,
    std::function<void(const std::shared_ptr<arrow::RecordBatch>&)>
        batch_handler) {
  while (true) {
    arrow::ArrayVector arrays;
    bool read_to_end = false;
    for (size_t i = 0; i < readers.size(); ++i) {
      auto chunked_arr = readers[i]->Next();
      if (chunked_arr == nullptr) {
        read_to_end = true;
        break;
      }

      const auto& target_field = schema->field(i);
      if (arrow::is_temporal(target_field->type()->id()) &&
          !chunked_arr->type()->Equals(target_field->type())) {
        auto cast_result =
            arrow::compute::Cast(chunked_arr, target_field->type());
        YACL_ENFORCE(cast_result.ok(), "Cast to {} failed for column {}: {}",
                     target_field->type()->ToString(), target_field->name(),
                     cast_result.status().ToString());
        chunked_arr = cast_result.ValueOrDie().chunked_array();
      }

      auto concatenate_result = arrow::Concatenate(chunked_arr->chunks());
      YACL_ENFORCE(concatenate_result.ok(),
                   "Failed to concatenate chunks for column {}: {}",
                   target_field->name(),
                   concatenate_result.status().ToString());
      arrays.emplace_back(concatenate_result.ValueOrDie());
    }

    if (read_to_end) {
      break;
    }

    int64_t length = arrays[0]->length();
    auto record_batch = arrow::RecordBatch::Make(schema, length, arrays);
    batch_handler(record_batch);
  }
}

void WriteToCsvStream(
    const std::shared_ptr<arrow::Schema>& schema,
    const arrow::csv::WriteOptions& options,
    const std::shared_ptr<arrow::io::OutputStream>& out_stream,
    const std::vector<std::shared_ptr<TensorBatchReader>>& readers) {
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  ASSIGN_OR_THROW_ARROW_STATUS(
      writer, arrow::csv::MakeCSVWriter(out_stream, schema, options));

  ProcessRecordBatchesFromTensors(
      schema, readers, [&](const std::shared_ptr<arrow::RecordBatch>& batch) {
        THROW_IF_ARROW_NOT_OK(writer->WriteRecordBatch(*batch));
      });

  THROW_IF_ARROW_NOT_OK(writer->Close());
}

void UploadViaDataProxy(dataproxy_sdk::proto::UploadInfo& upload_info,
                        const std::string& file_path) {
  dataproxy_sdk::proto::DataProxyConfig config;
  config.set_data_proxy_addr(FLAGS_kuscia_datamesh_endpoint);
  config.mutable_tls_config()->set_certificate_path(
      FLAGS_kuscia_datamesh_client_cert_path);
  config.mutable_tls_config()->set_private_key_path(
      FLAGS_kuscia_datamesh_client_key_path);
  config.mutable_tls_config()->set_ca_file_path(
      FLAGS_kuscia_datamesh_cacert_path);
  auto dp_file = dataproxy_sdk::DataProxyFile::Make(config);
  YACL_ENFORCE(dp_file != nullptr, "Failed to create DataProxyFile instance.");

  dp_file->UploadFile(upload_info, file_path,
                      dataproxy_sdk::proto::FileFormat::CSV);
}

void WriteDpDirectly(
    const std::string& table_name,
    const std::vector<std::shared_ptr<arrow::Field>>& fields,
    const std::vector<std::shared_ptr<TensorBatchReader>>& readers) {
  // 1. parse output_dataproxy_json
  datasource::DataProxyConf dp_conf;
  auto status = google::protobuf::util::JsonStringToMessage(
      FLAGS_output_dataproxy_json, &dp_conf);
  YACL_ENFORCE(status.ok(), "failed to parse json to dataproxy conf: error={}",
               status.ToString());
  YACL_ENFORCE(dp_conf.datasource().type() == "odps",
               "datasouce type: {} != 'odps', only support write odps now",
               dp_conf.datasource().type());

  kuscia::proto::api::v1alpha1::datamesh::CommandDataMeshUpdate command;
  command.mutable_datasource()->CopyFrom(dp_conf.datasource());
  command.mutable_update()->set_content_type(
      kuscia::proto::api::v1alpha1::datamesh::Table);
  auto* domaindata = command.mutable_domaindata();
  domaindata->set_type("table");  // TODO support more types
  domaindata->set_relative_uri(table_name);
  // TODO: support partitions
  for (const auto& field : fields) {
    auto column = domaindata->add_columns();
    column->set_name(field->name());
    column->set_type(scql::engine::util::ArrowToKusciaType(field->type()));
  }

  // 2. connect to dataproxy
  arrow::flight::Location location;
  ASSIGN_OR_THROW_ARROW_STATUS(
      location, arrow::flight::Location::Parse(dp_conf.dp_uri()));
  // TODO: support tls and more options
  arrow::flight::FlightClientOptions options;
  std::unique_ptr<arrow::flight::FlightClient> client;
  // TODO: reuse connection
  ASSIGN_OR_THROW_ARROW_STATUS(
      client, arrow::flight::FlightClient::Connect(location, options));

  google::protobuf::Any any_msg;
  any_msg.PackFrom(command);
  std::string serialized_data;
  YACL_ENFORCE(any_msg.SerializeToString(&serialized_data),
               "failed to serialize the flight command");
  auto descriptor = arrow::flight::FlightDescriptor::Command(serialized_data);
  std::unique_ptr<arrow::flight::FlightInfo> flight_info;
  ASSIGN_OR_THROW_ARROW_STATUS(flight_info, client->GetFlightInfo(descriptor));

  auto endpoionts = flight_info->endpoints();
  YACL_ENFORCE(!endpoionts.empty(), "endpoints should not be empty");
  auto dp_descriptor =
      arrow::flight::FlightDescriptor::Command(endpoionts[0].ticket.ticket);

  arrow::flight::FlightClient::DoPutResult put_result;
  ASSIGN_OR_THROW_ARROW_STATUS(
      put_result, client->DoPut(dp_descriptor, arrow::schema(fields)));

  // 3. write datas to dataproxy
  ProcessRecordBatchesFromTensors(
      arrow::schema(fields), readers,
      [&](const std::shared_ptr<arrow::RecordBatch>& batch) {
        THROW_IF_ARROW_NOT_OK(put_result.writer->WriteRecordBatch(*batch));
      });

  // 4. close writer
  THROW_IF_ARROW_NOT_OK(put_result.writer->Close());
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

  auto file_path = ctx->GetStringValueFromAttribute(kFilePathAttr);
  std::string dp_prefix = "dataproxy://";
  if (absl::StartsWithIgnoreCase(file_path, dp_prefix)) {
    // TODO: the WriteOptions should works when write csv through dataproxy,
    // currently only support write ODPS
    WriteDpDirectly(file_path.substr(dp_prefix.length()), fields, readers);
    ctx->GetSession()->SetAffectedRows(total_len);
    return;
  }

  auto schema = arrow::schema(fields);
  auto logger = ctx->GetActiveLogger();

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

  auto out_stream =
      BuildOutputStream(logger, file_path, FLAGS_enable_restricted_write_path,
                        FLAGS_restricted_write_path);
  YACL_ENFORCE(out_stream, "create output stream failed");

  WriteToCsvStream(schema, options, out_stream, readers);
  SPDLOG_LOGGER_INFO(logger, "write to csv stream completed");

  if (!FLAGS_dataproxy_upload_info.empty()) {
    SPDLOG_LOGGER_INFO(logger, "start uploading via DataProxy");
    std::string decoded_string;
    bool decode_success =
        butil::Base64Decode(FLAGS_dataproxy_upload_info, &decoded_string);
    YACL_ENFORCE(decode_success, "decode upload info failed");

    std::optional<dataproxy_sdk::pb::UploadInfo> parsed_upload_info_opt =
        scql::engine::util::ParseUploadInfoFromString(decoded_string);
    YACL_ENFORCE(
        parsed_upload_info_opt.has_value(),
        "Failed to parse FLAGS_dataproxy_upload_info. The string might be "
        "malformed or not represent a valid UploadInfo.");
    dataproxy_sdk::proto::UploadInfo upload_info =
        scql::engine::util::ConvertProtoToClass(parsed_upload_info_opt.value());

    scql::engine::util::ResetColumnsFromSchema(schema, upload_info);

    UploadViaDataProxy(upload_info, file_path);
    SPDLOG_LOGGER_INFO(logger, "upload via DataProxy completed");

    SPDLOG_LOGGER_INFO(logger, "start clean file {}", file_path);
    if (std::remove(file_path.c_str()) != 0) {
      SPDLOG_LOGGER_ERROR(logger, "failed to delete file: {}", file_path);
    } else {
      SPDLOG_LOGGER_INFO(logger, "file deleted successfully: {}", file_path);
    }
  }

  ctx->GetSession()->SetAffectedRows(total_len);
}
};  // namespace scql::engine::op
