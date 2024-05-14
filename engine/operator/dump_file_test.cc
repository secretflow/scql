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

#include "engine/operator/dump_file.h"

#include <cstdio>
#include <filesystem>

#include "butil/file_util.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

DECLARE_bool(enable_restricted_write_path);
DECLARE_string(restricted_write_path);

struct DumpFileTestCase {
  std::vector<test::NamedTensor> inputs;
  std::vector<std::string> output_names;
  std::string output_file_path;
  std::string line_terminator = "\n";
  std::string field_deliminator = ",";
  int64_t quoting = 1;
  std::string output_file_content;
};

class DumpFileTest : public ::testing::TestWithParam<DumpFileTestCase> {
 protected:
  static pb::ExecNode MakeDumpFileExecNode(const DumpFileTestCase& tc);

  static void FeedInputs(ExecContext* ctx, const DumpFileTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    DumpPrivateTest, DumpFileTest,
    testing::Values(
        DumpFileTestCase{
            .inputs =
                {test::NamedTensor(
                     "x1", TensorFromJSON(arrow::float64(),
                                          "[-3.1415, 0.1, 99.999, 1000]")),
                 test::NamedTensor("x2",
                                   TensorFromJSON(arrow::boolean(),
                                                  "[true,false,false,true]")),
                 test::NamedTensor(
                     "x3",
                     TensorFromJSON(arrow::large_utf8(),
                                    R"json(["test str","","A","B"])json"))},
            .output_names = {"x1_dump", "x2_dump", "x3_dump"},
            .output_file_path = "./dumpfile_out.1",
            .output_file_content = R"csv("x1_dump","x2_dump","x3_dump"
-3.1415,true,"test str"
0.1,false,""
99.999,false,"A"
1000,true,"B"
)csv"},
        DumpFileTestCase{
            .inputs =
                {test::NamedTensor(
                     "x1", TensorFromJSON(arrow::float64(),
                                          "[-3.1415, 0.1, 99.999, 1000]")),
                 test::NamedTensor("x2",
                                   TensorFromJSON(arrow::boolean(),
                                                  "[true,false,false,true]")),
                 test::NamedTensor(
                     "x3",
                     TensorFromJSON(arrow::large_utf8(),
                                    R"json(["test str","","A","B"])json"))},
            .output_names = {"x1_dump", "x2_dump", "x3_dump"},
            .output_file_path = "./dumpfile_out.2",
            .line_terminator = ";\n",
            .field_deliminator = "|",
            .quoting = 0,
            .output_file_content = R"csv("x1_dump"|"x2_dump"|"x3_dump";
-3.1415|true|test str;
0.1|false|;
99.999|false|A;
1000|true|B;
)csv"},
        DumpFileTestCase{
            .inputs = {test::NamedTensor(
                "x1", TensorFromJSON(arrow::large_utf8(),
                                     R"json(["D","C","","B","A"])json"))},
            .output_names = {"x1_dump"},
            .output_file_path = "./dumpfile_out.3",
            .output_file_content = R"csv("x1_dump"
"D"
"C"
""
"B"
"A"
)csv"},
        DumpFileTestCase{
            .inputs = {test::NamedTensor(
                "x1", TensorFromJSON(arrow::int64(), "[-1,0,1,2,3,10,11,12]"))},
            .output_names = {"x1_dump"},
            .output_file_path = "./dump_test/dumpfile_out.4",
            .output_file_content = R"csv("x1_dump"
-1
0
1
2
3
10
11
12
)csv"}));

TEST_P(DumpFileTest, works) {
  // Given
  FLAGS_restricted_write_path = std::filesystem::current_path().string();
  auto tc = GetParam();
  auto node = MakeDumpFileExecNode(tc);
  auto session = test::Make1PCSession();
  ExecContext ctx(node, session.get());

  FeedInputs(&ctx, tc);

  // When
  DumpFile op;
  ASSERT_NO_THROW(op.Run(&ctx));

  // Then
  std::string file_path =
      FLAGS_restricted_write_path + "/" + tc.output_file_path;
  std::string file_content;
  ASSERT_TRUE(
      butil::ReadFileToString(butil::FilePath(file_path), &file_content));
  ASSERT_EQ(file_content, tc.output_file_content);
  remove(file_path.c_str());
}

/// ===========================
/// DumpFileTest impl
/// ===========================

pb::ExecNode DumpFileTest::MakeDumpFileExecNode(const DumpFileTestCase& tc) {
  test::ExecNodeBuilder builder(DumpFile::kOpType);

  builder.SetNodeName("dump-file-test");
  builder.AddStringsAttr(DumpFile::kFilePathAttr,
                         std::vector<std::string>{tc.output_file_path});
  builder.AddStringsAttr(DumpFile::kLineTerminatorAttr,
                         std::vector<std::string>{tc.line_terminator});
  builder.AddStringsAttr(DumpFile::kFieldDeliminatorAttr,
                         std::vector<std::string>{tc.field_deliminator});
  builder.AddInt64Attr(DumpFile::kQuotingStyleAttr, tc.quoting);
  // Add inputs
  std::vector<pb::Tensor> input_datas;
  for (const auto& named_tensor : tc.inputs) {
    auto data = test::MakePrivateTensorReference(named_tensor.name,
                                                 named_tensor.tensor->Type());
    input_datas.push_back(std::move(data));
  }
  builder.AddInput(DumpFile::kIn, input_datas);

  // Add outputs
  std::vector<pb::Tensor> outputs;
  for (size_t i = 0; i < tc.output_names.size(); ++i) {
    auto out = test::MakeTensorAs(tc.output_names[i], input_datas[i]);
    out.set_option(pb::TensorOptions::VALUE);
    out.add_string_data(tc.output_names[i]);
    out.set_elem_type(pb::PrimitiveDataType::STRING);
    outputs.push_back(std::move(out));
  }
  builder.AddOutput(DumpFile::kOut, outputs);

  return builder.Build();
}

void DumpFileTest::FeedInputs(ExecContext* ctx, const DumpFileTestCase& tc) {
  test::FeedInputsAsPrivate(ctx, tc.inputs);
}

}  // namespace scql::engine::op
