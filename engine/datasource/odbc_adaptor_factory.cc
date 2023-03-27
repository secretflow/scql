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

#include "engine/datasource/odbc_adaptor_factory.h"

#include "engine/datasource/odbc_adaptor.h"

namespace scql::engine {

std::unique_ptr<DatasourceAdaptor> OdbcAdaptorFactory::CreateAdaptor(
    const DataSource& datasource_spec) {
  OdbcAdaptorOptions options;
  options.kind = datasource_spec.kind();
  options.connection_str = datasource_spec.connection_str();
  options.connection_type = connection_type_;
  options.pool_size = pool_size_;

  return std::make_unique<OdbcAdaptor>(options);
}

}  // namespace scql::engine