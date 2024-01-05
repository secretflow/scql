#include "engine/datasource/http_router.h"

#include "absl/strings/str_split.h"
#include "brpc/channel.h"
#include "google/protobuf/util/json_util.h"
#include "yacl/base/exception.h"

#include "engine/datasource/http_router.pb.h"

namespace scql::engine {

HttpRouter::HttpRouter(const HttpRouterOptions& options) : options_(options) {
  if (options_.endpoint.empty()) {
    throw std::invalid_argument("endpoint field of options cannot be empty");
  }
}

std::vector<DataSource> HttpRouter::Route(
    const std::vector<std::string>& table_refs) {
  // 1. build request
  ::router::RouteRequest request;

  for (const auto& table_ref : table_refs) {
    std::vector<std::string> fields = absl::StrSplit(table_ref, '.');
    if (fields.size() != 2) {
      YACL_THROW("invalid '$db.$table' format: {}", table_ref);
    }

    auto tb = request.add_tables();
    tb->set_db(fields[0]);
    tb->set_table(fields[1]);
  }

  // 2. issue rpc
  brpc::Channel channel;
  brpc::ChannelOptions channel_options;

  channel_options.protocol = "http";
  channel_options.timeout_ms = options_.timeout_ms;
  channel_options.max_retry = options_.max_retry;
  if (channel.Init(options_.endpoint.c_str(), "rr", &channel_options) != 0) {
    YACL_THROW("Failed to init channel to router with addr={}",
               options_.endpoint);
  }

  brpc::Controller cntl;

  cntl.http_request().uri() = options_.endpoint;
  cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
  cntl.http_request().set_content_type("application/json");

  {
    std::string req;
    auto status = google::protobuf::util::MessageToJsonString(request, &req);
    if (!status.ok()) {
      YACL_THROW(
          "Failed to converts request from protobuf message to JSON, reason={}",
          status.ToString());
    }

    cntl.request_attachment().append(req);
  }
  channel.CallMethod(nullptr, &cntl, nullptr, nullptr, nullptr);

  if (cntl.Failed()) {
    YACL_THROW("Failed to request http router service {}, code={}, msg={}",
               options_.endpoint, cntl.ErrorCode(), cntl.ErrorText());
  }

  ::router::RouteResponse response;
  {
    std::string res = cntl.response_attachment().to_string();
    google::protobuf::util::JsonParseOptions opts;
    opts.case_insensitive_enum_parsing = true;
    auto status = google::protobuf::util::JsonStringToMessage(
        google::protobuf::StringPiece(res), &response, opts);

    if (!status.ok()) {
      YACL_THROW("Failed to parse rpc response to RouteResponse, reason={}",
                 status.ToString());
    }
  }

  if (response.status().code() != 0) {
    YACL_THROW("router returns error: code={}, msg={}",
               response.status().code(), response.status().message());
  }

  if (table_refs.size() !=
      static_cast<size_t>(response.datasource_ids_size())) {
    YACL_THROW(
        "Mismatched size between response datasource_ids and request "
        "table_refs, expect "
        "{}, but got {}",
        table_refs.size(), response.datasource_ids_size());
  }

  std::vector<DataSource> result(table_refs.size());
  for (size_t i = 0; i < table_refs.size(); i++) {
    auto iter = response.datasources().find(response.datasource_ids(i));
    if (iter == response.datasources().end()) {
      YACL_THROW("Unable to find datasource id={} in response",
                 response.datasource_ids(i));
    }
    result[i].CopyFrom(iter->second);
  }
  return result;
}

}  // namespace scql::engine