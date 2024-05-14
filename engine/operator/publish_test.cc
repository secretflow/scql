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

#include "engine/operator/publish.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct PublishTestCase {
  std::vector<test::NamedTensor> inputs;
  std::vector<std::string> out_names;
  std::vector<std::string> out_strs;
};

class PublishTest : public ::testing::TestWithParam<PublishTestCase> {
 protected:
  static pb::ExecNode MakePublishExecNode(const PublishTestCase& tc);

  static void FeedInputs(ExecContext* ctxs, const PublishTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    PublishPrivateTest, PublishTest,
    testing::Values(
        PublishTestCase{
            .inputs = {test::NamedTensor(
                "private", TensorFromJSON(arrow::int64(), "[0,1,2,3,4]"))},
            .out_names = {"private_out"},
            .out_strs = {R"""(name: "private_out"
shape {
  dim {
    dim_value: 5
  }
  dim {
    dim_value: 1
  }
}
elem_type: INT64
annotation {
}
int64_data: 0
int64_data: 1
int64_data: 2
int64_data: 3
int64_data: 4
)"""}},
        PublishTestCase{
            .inputs = {test::NamedTensor(
                           "p0", TensorFromJSON(
                                     arrow::large_utf8(),
                                     R"json(["B","A","A","CCC","B"])json")),
                       test::NamedTensor(
                           "p1",
                           TensorFromJSON(
                               arrow::float32(),
                               "[1.1025, 100.245, -10.2, 0.34, 3.1415926]"))},
            .out_names = {"po0", "po1"},
            .out_strs = {R"""(name: "po0"
shape {
  dim {
    dim_value: 5
  }
  dim {
    dim_value: 1
  }
}
elem_type: STRING
annotation {
}
string_data: "B"
string_data: "A"
string_data: "A"
string_data: "CCC"
string_data: "B"
)""",
                         R"""(name: "po1"
shape {
  dim {
    dim_value: 5
  }
  dim {
    dim_value: 1
  }
}
elem_type: FLOAT32
annotation {
}
float_data: 1.1025
float_data: 100.245
float_data: -10.2
float_data: 0.34
float_data: 3.1415925
)"""}}));

TEST_P(PublishTest, Works) {
  // Give
  auto tc = GetParam();
  auto node = MakePublishExecNode(tc);
  auto session = test::Make1PCSession();
  ExecContext ctx(node, session.get());
  FeedInputs(&ctx, tc);

  // When
  Publish op;
  ASSERT_NO_THROW(op.Run(&ctx););

  // Then check output
  auto result = session->GetPublishResults();
  ASSERT_EQ(tc.inputs.size(), result.size());
  for (size_t i = 0; i < result.size(); ++i) {
    EXPECT_EQ(result[i]->DebugString(), tc.out_strs[i]);
  }
}

/// ===========================
/// PublishTest impl
/// ===========================

pb::ExecNode PublishTest::MakePublishExecNode(const PublishTestCase& tc) {
  test::ExecNodeBuilder builder(Publish::kOpType);

  builder.SetNodeName("publish-test");
  // Add inputs
  std::vector<pb::Tensor> input_datas;
  for (size_t i = 0; i < tc.inputs.size(); ++i) {
    const auto& named_tensor = tc.inputs[i];
    auto data = test::MakePrivateTensorReference(named_tensor.name,
                                                 named_tensor.tensor->Type());
    input_datas.push_back(std::move(data));
  }
  builder.AddInput(Publish::kIn, input_datas);

  // Add outputs
  std::vector<pb::Tensor> outputs;
  for (const auto& name : tc.out_names) {
    pb::Tensor out;

    out.set_name(name);
    out.set_option(pb::TensorOptions::VALUE);
    out.add_string_data(name);
    out.set_elem_type(pb::PrimitiveDataType::STRING);
    outputs.push_back(std::move(out));
  }
  builder.AddOutput(Publish::kOut, outputs);

  return builder.Build();
}

void PublishTest::FeedInputs(ExecContext* ctx, const PublishTestCase& tc) {
  test::FeedInputsAsPrivate(ctx, tc.inputs);
}

}  // namespace scql::engine::op