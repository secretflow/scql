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

#include "engine/datasource/datasource_adaptor_mgr.h"

#include "yacl/base/exception.h"

#include "engine/datasource/arrow_sql_adaptor_factory.h"
#include "engine/datasource/csvdb_adaptor_factory.h"
#include "engine/datasource/odbc_adaptor_factory.h"

namespace scql::engine {

DatasourceAdaptorMgr::DatasourceAdaptorMgr() {
  RegisterBuiltinAdaptorFactories();
}

std::shared_ptr<DatasourceAdaptor> DatasourceAdaptorMgr::GetAdaptor(
    const DataSource& datasource_spec) {
  // CSVDB may change frequently, so avoid to store in map.
  if (datasource_spec.kind() == DataSourceKind::CSVDB) {
    return CreateAdaptor(datasource_spec);
  }

  auto spec_pair =
      std::pair(datasource_spec.connection_str(), datasource_spec.kind());
  {
    absl::ReaderMutexLock lock(&mu_);

    auto iter = adaptors_.find(spec_pair);
    if (iter != adaptors_.end()) {
      return iter->second;
    }
  }
  // not existed, or datasource specification updated
  std::shared_ptr<DatasourceAdaptor> adaptor = CreateAdaptor(datasource_spec);
  YACL_ENFORCE(adaptor, "unsupported datasource: {}",
               datasource_spec.ShortDebugString());

  {
    absl::MutexLock lock(&mu_);
    adaptors_[spec_pair] = adaptor;
  }
  return adaptor;
}

std::shared_ptr<DatasourceAdaptor> DatasourceAdaptorMgr::CreateAdaptor(
    const DataSource& datasource_spec) {
  auto iter = factory_maps_.find(datasource_spec.kind());
  if (iter == factory_maps_.end()) {
    return nullptr;
  }
  auto& factory = iter->second;
  return factory->CreateAdaptor(datasource_spec);
}

void DatasourceAdaptorMgr::RegisterBuiltinAdaptorFactories() {
  auto odbc_adaptor_factory = std::make_shared<OdbcAdaptorFactory>();
  factory_maps_.insert({DataSourceKind::MYSQL, odbc_adaptor_factory});
  factory_maps_.insert({DataSourceKind::SQLITE, odbc_adaptor_factory});
  factory_maps_.insert({DataSourceKind::POSTGRESQL, odbc_adaptor_factory});
  factory_maps_.insert(
      {DataSourceKind::CSVDB, std::make_shared<CsvdbAdaptorFactory>()});
  factory_maps_.insert(
      {DataSourceKind::ARROWSQL, std::make_shared<ArrowSqlAdaptorFactory>()});
}

}  // namespace scql::engine