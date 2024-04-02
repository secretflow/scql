#pragma once

#include "grpcpp/channel.h"
#include "grpcpp/security/credentials.h"

#include "engine/datasource/router.h"

namespace scql::engine {

class KusciaDataMeshRouter final : public Router {
 public:
  KusciaDataMeshRouter(
      const std::string& endpoint,
      const std::shared_ptr<grpc::ChannelCredentials>& credentials);

  std::vector<DataSource> Route(
      const std::vector<std::string>& table_refs) override;

 private:
  DataSource SingleRoute(std::shared_ptr<grpc::Channel> channel,
                         const std::string& domaindata_id);

 private:
  const std::string endpoint_;
  std::shared_ptr<grpc::ChannelCredentials> creds_;
};

};  // namespace scql::engine