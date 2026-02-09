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

#include "engine/util/datamesh_helper.h"

#include <gmock/gmock-matchers.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include "yacl/base/exception.h"

#include "kuscia/proto/api/v1alpha1/datamesh/domaindata.grpc.pb.h"
#include "kuscia/proto/api/v1alpha1/datamesh/domaindatasource.grpc.pb.h"

namespace scql::engine::util {

class FakeDomainDataService : public dm::DomainDataService::Service {
 public:
  void SetNextResponse(const dm::QueryDomainDataResponse& response,
                       const grpc::Status& status) {
    std::lock_guard<std::mutex> lock(mu_);
    next_response_ = response;
    next_status_ = status;
    called_ = false;
  }

  grpc::Status QueryDomainData(grpc::ServerContext* context,
                               const dm::QueryDomainDataRequest* request,
                               dm::QueryDomainDataResponse* response) override {
    std::lock_guard<std::mutex> lock(mu_);
    called_ = true;
    if (request != nullptr) {
      last_request_ = *request;
    }
    if (response != nullptr) {
      *response = next_response_;
    }
    return next_status_;
  }

  bool WasCalled() {
    std::lock_guard<std::mutex> lock(mu_);
    return called_;
  }
  dm::QueryDomainDataRequest GetLastRequest() {
    std::lock_guard<std::mutex> lock(mu_);
    return last_request_;
  }

 private:
  std::mutex mu_;
  dm::QueryDomainDataResponse next_response_;
  grpc::Status next_status_ = grpc::Status::OK;
  bool called_ = false;
  dm::QueryDomainDataRequest last_request_;
};

class FakeDomainDataSourceService
    : public dm::DomainDataSourceService::Service {
 public:
  void SetNextResponse(const dm::QueryDomainDataSourceResponse& response,
                       const grpc::Status& status) {
    std::lock_guard<std::mutex> lock(mu_);
    next_response_ = response;
    next_status_ = status;
    called_ = false;
  }

  grpc::Status QueryDomainDataSource(
      grpc::ServerContext* context,
      const dm::QueryDomainDataSourceRequest* request,
      dm::QueryDomainDataSourceResponse* response) override {
    std::lock_guard<std::mutex> lock(mu_);
    called_ = true;
    if (request != nullptr) {
      last_request_ = *request;
    }
    if (response != nullptr) {
      *response = next_response_;
    }
    return next_status_;
  }

  bool WasCalled() {
    std::lock_guard<std::mutex> lock(mu_);
    return called_;
  }
  dm::QueryDomainDataSourceRequest GetLastRequest() {
    std::lock_guard<std::mutex> lock(mu_);
    return last_request_;
  }

 private:
  std::mutex mu_;
  dm::QueryDomainDataSourceResponse next_response_;
  grpc::Status next_status_ = grpc::Status::OK;
  bool called_ = false;
  dm::QueryDomainDataSourceRequest last_request_;
};

class DataMeshHelperTest : public ::testing::Test {
 protected:
  std::unique_ptr<grpc::Server> server_;
  std::shared_ptr<grpc::Channel> channel_;

  FakeDomainDataService fake_domain_data_service_;
  FakeDomainDataSourceService fake_domain_data_source_service_;

  std::string inproc_server_name_ = "datamesh_helper_test_server";

  void SetUp() override {
    grpc::ServerBuilder builder;

    builder.RegisterService(&fake_domain_data_service_);
    builder.RegisterService(&fake_domain_data_source_service_);

    // Build and start the server (for in-process, no network ports are listened
    // to here)
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr)
        << "Fake gRPC (in-process) server startup failure.";

    // Create a channel to the in-process server.
    channel_ = server_->InProcessChannel(grpc::ChannelArguments());
    ASSERT_NE(channel_, nullptr)
        << "Failed to create a channel to the in-process server.";
  }

  void TearDown() override {
    if (server_) {
      server_->Shutdown();
      server_->Wait();
    }
  }
};

// --- QueryDomainData Test Cases ---

TEST_F(DataMeshHelperTest, QueryDomainData_Success) {
  // Arrange
  std::string domain_id = "test_domain_success";
  dm::QueryDomainDataResponse expected_response_proto;
  expected_response_proto.mutable_status()->set_code(0);
  dm::DomainData* data_payload = expected_response_proto.mutable_data();
  data_payload->set_domaindata_id(domain_id);

  fake_domain_data_service_.SetNextResponse(expected_response_proto,
                                            grpc::Status::OK);

  // Act
  dm::DomainData result = QueryDomainData(channel_, domain_id);

  // Assert
  ASSERT_TRUE(fake_domain_data_service_.WasCalled());
  EXPECT_EQ(fake_domain_data_service_.GetLastRequest().domaindata_id(),
            domain_id);
  EXPECT_EQ(result.domaindata_id(), domain_id);
}

TEST_F(DataMeshHelperTest, QueryDomainData_GrpcError) {
  // Arrange
  std::string domain_id = "domain_grpc_failure";
  fake_domain_data_service_.SetNextResponse(
      {}, grpc::Status(grpc::StatusCode::UNAVAILABLE,
                       "Simulated gRPC network error"));

  // Act & Assert
  EXPECT_THROW(
      {
        try {
          QueryDomainData(channel_, domain_id);
        } catch (const yacl::Exception& e) {
          EXPECT_THAT(e.what(),
                      ::testing::HasSubstr("Simulated gRPC network error"));
          EXPECT_THAT(e.what(),
                      ::testing::HasSubstr(std::to_string(
                          static_cast<int>(grpc::StatusCode::UNAVAILABLE))));
          throw;
        }
      },
      yacl::Exception);
  ASSERT_TRUE(fake_domain_data_service_.WasCalled());
}

TEST_F(DataMeshHelperTest, QueryDomainData_ResponseStatusError) {
  // Arrange
  std::string domain_id = "domain_logical_error";
  dm::QueryDomainDataResponse error_response_proto;
  error_response_proto.mutable_status()->set_code(
      5);  // randomly pick a number that is not zero
  error_response_proto.mutable_status()->set_message(
      "Domain ID not found in system");

  fake_domain_data_service_.SetNextResponse(error_response_proto,
                                            grpc::Status::OK);

  // Act & Assert
  EXPECT_THROW(
      {
        try {
          QueryDomainData(channel_, domain_id);
        } catch (const yacl::Exception& e) {
          EXPECT_THAT(e.what(),
                      ::testing::HasSubstr("Domain ID not found in system"));
          EXPECT_THAT(e.what(), ::testing::HasSubstr("code=5"));
          throw;
        }
      },
      yacl::Exception);
  ASSERT_TRUE(fake_domain_data_service_.WasCalled());
}

// --- QueryDomainDataSource Test Cases ---

TEST_F(DataMeshHelperTest, QueryDomainDataSource_Success) {
  // Arrange
  std::string datasource_id = "ds_test_success";
  dm::QueryDomainDataSourceResponse expected_response_proto;
  expected_response_proto.mutable_status()->set_code(0);
  dm::DomainDataSource* data_payload = expected_response_proto.mutable_data();
  data_payload->set_datasource_id(datasource_id);
  data_payload->set_name("MySQL DB");
  data_payload->set_type("localfs");

  fake_domain_data_source_service_.SetNextResponse(expected_response_proto,
                                                   grpc::Status::OK);

  // Act
  dm::DomainDataSource result = QueryDomainDataSource(channel_, datasource_id);

  // Assert
  ASSERT_TRUE(fake_domain_data_source_service_.WasCalled());
  EXPECT_EQ(fake_domain_data_source_service_.GetLastRequest().datasource_id(),
            datasource_id);

  EXPECT_EQ(result.datasource_id(), datasource_id);
  EXPECT_EQ(result.name(), "MySQL DB");
  EXPECT_EQ(result.type(), "localfs");
}

TEST_F(DataMeshHelperTest, QueryDomainDataSource_GrpcError) {
  // Arrange
  std::string datasource_id = "ds_grpc_failure";
  fake_domain_data_source_service_.SetNextResponse(
      {}, grpc::Status(grpc::StatusCode::INTERNAL, "Unexpected server fault"));

  // Act & Assert
  EXPECT_THROW(
      {
        try {
          QueryDomainDataSource(channel_, datasource_id);
        } catch (const yacl::Exception& e) {
          EXPECT_THAT(e.what(),
                      ::testing::HasSubstr("Unexpected server fault"));
          EXPECT_THAT(e.what(),
                      ::testing::HasSubstr(std::to_string(
                          static_cast<int>(grpc::StatusCode::INTERNAL))));
          throw;
        }
      },
      yacl::Exception);
  ASSERT_TRUE(fake_domain_data_source_service_.WasCalled());
}

TEST_F(DataMeshHelperTest, QueryDomainDataSource_ResponseStatusError) {
  // Arrange
  std::string datasource_id = "ds_logical_error";
  dm::QueryDomainDataSourceResponse error_response_proto;
  error_response_proto.mutable_status()->set_code(12);
  error_response_proto.mutable_status()->set_message(
      "Datasource credentials invalid");

  fake_domain_data_source_service_.SetNextResponse(error_response_proto,
                                                   grpc::Status::OK);

  // Act & Assert
  EXPECT_THROW(
      {
        try {
          QueryDomainDataSource(channel_, datasource_id);
        } catch (const yacl::Exception& e) {
          EXPECT_THAT(e.what(),
                      ::testing::HasSubstr("Datasource credentials invalid"));
          EXPECT_THAT(e.what(), ::testing::HasSubstr("code=12"));
          throw;
        }
      },
      yacl::Exception);
  ASSERT_TRUE(fake_domain_data_source_service_.WasCalled());
}
}  // namespace scql::engine::util
