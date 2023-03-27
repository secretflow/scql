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

#include "engine/core/tensor_from_json.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct PublishTestCase {
  std::vector<test::NamedTensor> inputs;
  std::vector<pb::TensorStatus> in_status;
  std::vector<std::string> out_names;
  std::vector<std::string> out_strs;
};

class PublishTest : public ::testing::TestWithParam<
                        std::tuple<spu::ProtocolKind, PublishTestCase>> {
 protected:
  static pb::ExecNode MakePublishExecNode(const PublishTestCase& tc);

  static void FeedInputs(const std::vector<ExecContext*>& ctxs,
                         const PublishTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    PublishPrivateTest, PublishTest,
    testing::Combine(
        testing::Values(spu::ProtocolKind::CHEETAH, spu::ProtocolKind::SEMI2K),
        testing::Values(
            PublishTestCase{
                .inputs = {test::NamedTensor(
                    "private", TensorFromJSON(arrow::int64(), "[0,1,2,3,4]"))},
                .in_status = {pb::TENSORSTATUS_PRIVATE},
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
i64s {
  i64s: 0
  i64s: 1
  i64s: 2
  i64s: 3
  i64s: 4
}
)"""}},
            PublishTestCase{
                .inputs =
                    {test::NamedTensor(
                         "p0", TensorFromJSON(
                                   arrow::utf8(),
                                   R"json(["B","A","A","CCC","B","B"])json")),
                     test::NamedTensor(
                         "p1",
                         TensorFromJSON(
                             arrow::float32(),
                             "[1.1025, 100.245, -10.2, 0.34, 3.1415926]"))},
                .in_status = {pb::TENSORSTATUS_PRIVATE,
                              pb::TENSORSTATUS_PUBLIC},
                .out_names = {"po0", "po1"},
                .out_strs = {R"""(name: "po0"
shape {
  dim {
    dim_value: 6
  }
  dim {
    dim_value: 1
  }
}
elem_type: STRING
annotation {
}
ss {
  ss: "B"
  ss: "A"
  ss: "A"
  ss: "CCC"
  ss: "B"
  ss: "B"
}
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
elem_type: FLOAT
annotation {
}
fs {
  fs: 1.1024971
  fs: 100.245
  fs: -10.1999969
  fs: 0.339996338
  fs: 3.14159
}
)"""}})),
    TestParamNameGenerator(PublishTest));

TEST_P(PublishTest, Works) {
  // Give
  auto parm = GetParam();
  auto tc = std::get<1>(parm);
  pb::ExecNode node = MakePublishExecNode(tc);
  std::vector<Session> sessions = test::Make2PCSession(std::get<0>(parm));

  ExecContext alice_ctx(node, &sessions[0]);
  ExecContext bob_ctx(node, &sessions[1]);

  FeedInputs({&alice_ctx, &bob_ctx}, tc);

  // When
  Publish op;
  ASSERT_NO_THROW({ op.Run(&alice_ctx); });
  // Then
  // check output
  auto result = sessions[0].GetPublishResults();
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
  for (size_t i = 0; i < tc.in_status.size(); ++i) {
    const auto& named_tensor = tc.inputs[i];
    auto data = test::MakeTensorReference(
        named_tensor.name, named_tensor.tensor->Type(), tc.in_status[i]);
    input_datas.push_back(std::move(data));
  }
  builder.AddInput(Publish::kIn, input_datas);

  // Add outputs
  std::vector<pb::Tensor> outputs;
  for (const auto& name : tc.out_names) {
    pb::Tensor out;
    // only name is necessary to specify output column's name.
    out.set_name(name);
    outputs.push_back(std::move(out));
  }
  builder.AddOutput(Publish::kOut, outputs);

  return builder.Build();
}

void PublishTest::FeedInputs(const std::vector<ExecContext*>& ctxs,
                             const PublishTestCase& tc) {
  for (size_t i = 0; i < tc.in_status.size(); ++i) {
    const auto& named_tensor = tc.inputs[i];
    if (tc.in_status[i] == pb::TENSORSTATUS_PRIVATE) {
      test::FeedInputsAsPrivate(ctxs[0], {named_tensor});
    } else if (tc.in_status[i] == pb::TENSORSTATUS_PUBLIC) {
      test::FeedInputsAsPublic(ctxs, {named_tensor});
    }
  }
}

}  // namespace scql::engine::op