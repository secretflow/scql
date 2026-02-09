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

#include "engine/util/upload_info_helper.h"

#include "gtest/gtest.h"

namespace scql_proto = dataproxy_sdk::pb;
namespace dp_sdk = dataproxy_sdk::proto;
namespace scql_util = scql::engine::util;

class UploadInfoHelperTest : public ::testing::Test {
 protected:
  scql_proto::UploadInfo CreateSampleProtoUploadInfo() {
    scql_proto::UploadInfo proto_info;
    proto_info.set_domaindata_id("proto_ddid_001");
    proto_info.set_name("Sample Proto Info");
    proto_info.set_type("table");
    proto_info.set_relative_uri("data/sample_table.csv");
    proto_info.set_datasource_id("ds_main");
    proto_info.set_vendor("SCQL_Engine");

    (*proto_info.mutable_attributes())["source_system"] = "SystemA";
    (*proto_info.mutable_attributes())["version"] = "1.0";

    auto* col1 = proto_info.add_columns();
    col1->set_name("id");
    col1->set_type("int64");
    col1->set_comment("Primary key column");
    col1->set_not_nullable(true);

    auto* col2 = proto_info.add_columns();
    col2->set_name("description");
    col2->set_type("string");
    col2->set_comment("Text description, can be null");
    col2->set_not_nullable(false);

    return proto_info;
  }
};

TEST_F(UploadInfoHelperTest, ParseFromStringWithValidData) {
  scql_proto::UploadInfo original_proto = CreateSampleProtoUploadInfo();
  std::string serialized_data;
  ASSERT_TRUE(original_proto.SerializeToString(&serialized_data));

  std::optional<scql_proto::UploadInfo> parsed_opt =
      scql_util::ParseUploadInfoFromString(serialized_data);

  ASSERT_TRUE(parsed_opt.has_value());
  const auto& parsed_proto = parsed_opt.value();

  EXPECT_EQ(parsed_proto.domaindata_id(), original_proto.domaindata_id());
  EXPECT_EQ(parsed_proto.name(), original_proto.name());
  EXPECT_EQ(parsed_proto.type(), original_proto.type());
  EXPECT_EQ(parsed_proto.relative_uri(), original_proto.relative_uri());
  EXPECT_EQ(parsed_proto.datasource_id(), original_proto.datasource_id());
  EXPECT_EQ(parsed_proto.vendor(), original_proto.vendor());

  EXPECT_EQ(parsed_proto.attributes_size(), original_proto.attributes_size());
  EXPECT_EQ(parsed_proto.attributes().at("source_system"),
            original_proto.attributes().at("source_system"));

  ASSERT_EQ(parsed_proto.columns_size(), original_proto.columns_size());
  EXPECT_EQ(parsed_proto.columns(0).name(), original_proto.columns(0).name());
  EXPECT_EQ(parsed_proto.columns(0).not_nullable(),
            original_proto.columns(0).not_nullable());
  EXPECT_EQ(parsed_proto.columns(1).name(), original_proto.columns(1).name());
  EXPECT_EQ(parsed_proto.columns(1).not_nullable(),
            original_proto.columns(1).not_nullable());
}

TEST_F(UploadInfoHelperTest, ConvertProtoToCustomClassFullData) {
  scql_proto::UploadInfo source_proto = CreateSampleProtoUploadInfo();

  dp_sdk::UploadInfo target_custom_info =
      scql_util::ConvertProtoToClass(source_proto);

  EXPECT_EQ(target_custom_info.domaindata_id(), source_proto.domaindata_id());
  EXPECT_EQ(target_custom_info.name(), source_proto.name());
  EXPECT_EQ(target_custom_info.type(), source_proto.type());
  EXPECT_EQ(target_custom_info.relative_uri(), source_proto.relative_uri());
  EXPECT_EQ(target_custom_info.datasource_id(), source_proto.datasource_id());
  EXPECT_EQ(target_custom_info.vendor(), source_proto.vendor());

  ASSERT_EQ(target_custom_info.attributes_size(),
            source_proto.attributes_size());
  EXPECT_EQ(target_custom_info.attributes().at("source_system"),
            source_proto.attributes().at("source_system"));
  EXPECT_EQ(target_custom_info.attributes().at("version"),
            source_proto.attributes().at("version"));

  ASSERT_EQ(target_custom_info.columns_size(), source_proto.columns_size());
  ASSERT_EQ(target_custom_info.columns().size(),
            static_cast<size_t>(source_proto.columns_size()));

  const auto& custom_cols = target_custom_info.columns();

  ASSERT_GE(custom_cols.size(), 1);
  EXPECT_EQ(custom_cols[0].name(), source_proto.columns(0).name());
  EXPECT_EQ(custom_cols[0].type(), source_proto.columns(0).type());
  EXPECT_EQ(custom_cols[0].comment(), source_proto.columns(0).comment());
  EXPECT_EQ(custom_cols[0].not_nullable(),
            source_proto.columns(0).not_nullable());
  EXPECT_TRUE(custom_cols[0].not_nullable());

  ASSERT_GE(custom_cols.size(), 2);
  EXPECT_EQ(custom_cols[1].name(), source_proto.columns(1).name());
  EXPECT_EQ(custom_cols[1].type(), source_proto.columns(1).type());
  EXPECT_EQ(custom_cols[1].comment(), source_proto.columns(1).comment());
  EXPECT_EQ(custom_cols[1].not_nullable(),
            source_proto.columns(1).not_nullable());
  EXPECT_FALSE(custom_cols[1].not_nullable());
}

TEST_F(UploadInfoHelperTest, ResetColumnsFromSchemaTest) {
  dp_sdk::UploadInfo upload_info;
  auto* col1 = upload_info.add_columns();
  col1->set_name("id");
  col1->set_type("int64");
  col1->set_not_nullable(true);

  auto* col2 = upload_info.add_columns();
  col2->set_name("obsolete_column");
  col2->set_type("string");
  col2->set_not_nullable(false);

  auto field1 = arrow::field("id", arrow::int64(), false);
  auto field2 = arrow::field("float_val", arrow::float32(), true);
  auto field3 = arrow::field("double_val", arrow::float64(), true);
  auto field4 = arrow::field("string_val", arrow::large_utf8(), true);
  auto schema = arrow::schema({field1, field2, field3, field4});

  scql_util::ResetColumnsFromSchema(schema, upload_info);

  ASSERT_EQ(upload_info.columns_size(), 4);

  const auto& synced_cols = upload_info.columns();
  EXPECT_EQ(synced_cols[0].name(), "id");
  EXPECT_EQ(synced_cols[0].type(), "int64");
  EXPECT_TRUE(synced_cols[0].not_nullable());

  EXPECT_EQ(synced_cols[1].name(), "float_val");
  EXPECT_EQ(synced_cols[1].type(), "float32");
  EXPECT_FALSE(synced_cols[1].not_nullable());

  EXPECT_EQ(synced_cols[2].name(), "double_val");
  EXPECT_EQ(synced_cols[2].type(), "float64");
  EXPECT_FALSE(synced_cols[2].not_nullable());

  EXPECT_EQ(synced_cols[3].name(), "string_val");
  EXPECT_EQ(synced_cols[3].type(), "string");
  EXPECT_FALSE(synced_cols[3].not_nullable());
}