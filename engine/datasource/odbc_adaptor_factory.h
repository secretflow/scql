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

#include "engine/datasource/datasource_adaptor_factory.h"
#include "engine/datasource/odbc_adaptor.h"

namespace scql::engine {

class OdbcAdaptorFactory final : public DatasourceAdaptorFactory {
 public:
  OdbcAdaptorFactory() : OdbcAdaptorFactory(ConnectionType::Short, 0) {}

  OdbcAdaptorFactory(ConnectionType connection_type, std::size_t pool_size)
      : connection_type_(connection_type), pool_size_(pool_size) {}

  std::unique_ptr<DatasourceAdaptor> CreateAdaptor(
      const DataSource& datasource_spec) override;

 private:
  ConnectionType connection_type_;
  size_t pool_size_;
};

}  // namespace scql::engine