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

#pragma once

#include "engine/datasource/datasource_adaptor.h"

#include "engine/datasource/csvdb_conf.pb.h"
#include "engine/datasource/datasource.pb.h"

namespace scql::engine {

class CsvdbAdaptor : public DatasourceAdaptor {
 public:
  explicit CsvdbAdaptor(const std::string& json_str);

  ~CsvdbAdaptor() override = default;

 private:
  std::vector<TensorPtr> GetQueryResult(
      const std::string& query, const TensorBuildOptions& options) override;

  csv::CsvdbConf csvdb_conf_;
};

}  // namespace scql::engine
