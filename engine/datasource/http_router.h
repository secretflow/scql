#pragma once

#include "engine/datasource/router.h"

namespace scql::engine {

struct HttpRouterOptions {
  std::string endpoint;
  int32_t timeout_ms = 3000;
  int max_retry = 3;
};

class HttpRouter final : public Router {
 public:
  HttpRouter(const HttpRouterOptions& options);

  std::vector<DataSource> Route(
      const std::vector<std::string>& table_refs) override;

 private:
  const HttpRouterOptions options_;
};

}  // namespace scql::engine