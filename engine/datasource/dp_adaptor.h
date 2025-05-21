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

#pragma once

#include <future>
#include <utility>

#include "arrow/type_fwd.h"
#include "dataproxy_sdk/data_proxy_stream.h"

#include "engine/datasource/datasource_adaptor.h"

#include "engine/datasource/datasource.pb.h"

namespace scql::engine {

class DpChunkedResult : public ChunkedResult {
 public:
  DpChunkedResult(
      std::unique_ptr<dataproxy_sdk::DataProxyStreamReader> stream_reader)
      : stream_reader_(std::move(stream_reader)) {}

  std::optional<arrow::ChunkedArrayVector> Fetch() override;
  std::shared_ptr<arrow::Schema> GetSchema() override {
    return stream_reader_->Schema();
  };

 private:
  std::unique_ptr<dataproxy_sdk::DataProxyStreamReader> stream_reader_;
};
class DpAdaptor : public DatasourceAdaptor {
 public:
  explicit DpAdaptor(std::string datasource_id)
      : datasource_id_(std::move(datasource_id)) {}

  ~DpAdaptor() override = default;

 private:
  // Send query and return schema
  std::shared_ptr<ChunkedResult> SendQuery(const std::string& query) override;

  std::string datasource_id_;
};

}  // namespace scql::engine
