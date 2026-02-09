// Copyright 2025 Ant Group Co., Ltd.
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

#include <utility>

#include "arrow/flight/client.h"
#include "arrow/type_fwd.h"

#include "engine/datasource/datasource_adaptor.h"

#include "engine/util/dp/flight.pb.h"

namespace scql::engine {

using kuscia::proto::api::v1alpha1::datamesh::CommandDataMeshSqlQuery;

class DpChunkedResult : public ChunkedResult {
 public:
  DpChunkedResult(std::shared_ptr<arrow::Schema> schema,
                  std::unique_ptr<arrow::flight::FlightInfo> flight_info,
                  arrow::flight::FlightClient* client_ptr)
      : schema_(std::move(schema)),
        flight_info_(std::move(flight_info)),
        client_(client_ptr) {}

  std::optional<arrow::ChunkedArrayVector> Fetch() override;
  std::shared_ptr<arrow::Schema> GetSchema() override { return schema_; };

 private:
  std::shared_ptr<arrow::Schema> schema_;
  std::unique_ptr<arrow::flight::FlightInfo> flight_info_;
  size_t endpoint_index_ = 0;
  arrow::flight::FlightClient* client_ = nullptr;
};

class DpAdaptor : public DatasourceAdaptor {
 public:
  explicit DpAdaptor(std::string json_str);

  ~DpAdaptor() override = default;

 private:
  // Send query and return schema
  std::shared_ptr<ChunkedResult> SendQuery(const std::string& query) override;

  CommandDataMeshSqlQuery command_;
  std::unique_ptr<arrow::flight::FlightClient> client_;
};

}  // namespace scql::engine
