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

#include <utility>

#include "engine/datasource/datasource_adaptor.h"

#include "engine/datasource/datasource.pb.h"

namespace scql::engine {

namespace Dataproxy {
// convert datetime to int64
// return input for other datatype
std::shared_ptr<arrow::ChunkedArray> ConvertDatetimeToInt64(
    const std::shared_ptr<arrow::ChunkedArray>& array);
}  // namespace Dataproxy

class DpAdaptor : public DatasourceAdaptor {
 public:
  explicit DpAdaptor(std::string datasource_id)
      : datasource_id_(std::move(datasource_id)) {}

  ~DpAdaptor() override = default;

 private:
  std::vector<TensorPtr> GetQueryResult(
      const std::string& query, const TensorBuildOptions& options) override;

  std::string datasource_id_;
};

}  // namespace scql::engine
