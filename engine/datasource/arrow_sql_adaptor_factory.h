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

#include "engine/datasource/arrow_sql_adaptor.h"
#include "engine/datasource/datasource_adaptor_factory.h"

namespace scql::engine {

class ArrowSqlAdaptorFactory final : public DatasourceAdaptorFactory {
 public:
  std::unique_ptr<DatasourceAdaptor> CreateAdaptor(
      const DataSource& datasource_spec) override {
    return std::make_unique<ArrowSqlAdaptor>(datasource_spec.connection_str());
  }
};

}  // namespace scql::engine