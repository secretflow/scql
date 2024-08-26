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

#include "engine/operator/constant.h"

#include "gtest/gtest.h"

#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

struct ConstantTestCase {
  test::NamedTensor scalar;
  pb::TensorStatus output_status;
};

class ConstantTest
    : public ::testing::TestWithParam<
          std::tuple<test::SpuRuntimeTestCase, ConstantTestCase>> {
 protected:
  static pb::ExecNode MakeConstantExecNode(const ConstantTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    ConstantPrivateTest, ConstantTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            ConstantTestCase{.scalar = {test::NamedTensor(
                                 "x", TensorFrom(arrow::int64(), "[3]"))},
                             .output_status = pb::TENSORSTATUS_PRIVATE},
            ConstantTestCase{.scalar = {test::NamedTensor(
                                 "y", TensorFrom(arrow::large_utf8(),
                                                 R"json(["2022-11-22"])json"))},
                             .output_status = pb::TENSORSTATUS_PRIVATE},
            ConstantTestCase{.scalar = {test::NamedTensor(
                                 "z", TensorFrom(arrow::boolean(), "[true]"))},
                             .output_status = pb::TENSORSTATUS_PRIVATE},
            ConstantTestCase{
                .scalar = {test::NamedTensor("zz", TensorFrom(arrow::float32(),
                                                              "[3.1415]"))},
                .output_status = pb::TENSORSTATUS_PRIVATE})),
    TestParamNameGenerator(ConstantTest));

INSTANTIATE_TEST_SUITE_P(
    ConstantPublicTest, ConstantTest,
    testing::Combine(
        test::SpuTestValuesMultiPC,
        testing::Values(
            ConstantTestCase{.scalar = {test::NamedTensor(
                                 "x", TensorFrom(arrow::int64(), "[3]"))},
                             .output_status = pb::TENSORSTATUS_PUBLIC},
            ConstantTestCase{.scalar = {test::NamedTensor(
                                 "y", TensorFrom(arrow::large_utf8(),
                                                 R"json(["2022-11-22"])json"))},
                             .output_status = pb::TENSORSTATUS_PUBLIC},
            ConstantTestCase{.scalar = {test::NamedTensor(
                                 "z", TensorFrom(arrow::boolean(), "[true]"))},
                             .output_status = pb::TENSORSTATUS_PUBLIC},
            ConstantTestCase{
                .scalar = {test::NamedTensor("zz", TensorFrom(arrow::float32(),
                                                              "[3.1415]"))},
                .output_status = pb::TENSORSTATUS_PUBLIC})),
    TestParamNameGenerator(ConstantTest));

TEST_P(ConstantTest, Works) {
  // Given
  auto parm = GetParam();
  auto tc = std::get<1>(parm);
  auto node = MakeConstantExecNode(tc);

  auto sessions = test::MakeMultiPCSession(std::get<0>(parm));

  std::vector<ExecContext> exec_ctxs;
  for (size_t idx = 0; idx < sessions.size(); ++idx) {
    exec_ctxs.emplace_back(node, sessions[idx].get());
  }

  std::vector<ExecContext*> ctx_ptrs;
  for (size_t idx = 0; idx < exec_ctxs.size(); ++idx) {
    ctx_ptrs.emplace_back(&exec_ctxs[idx]);
  }

  // When
  EXPECT_NO_THROW(test::RunAsync<Constant>(ctx_ptrs));

  // check alice output
  auto expect_arr = tc.scalar.tensor->ToArrowChunkedArray();
  TensorPtr out;
  if (tc.output_status == pb::TENSORSTATUS_PRIVATE) {
    out = exec_ctxs[0].GetTensorTable()->GetTensor(tc.scalar.name);
  } else {
    auto sctx = exec_ctxs[0].GetSession()->GetSpuContext();
    auto device_symbols = exec_ctxs[0].GetSession()->GetDeviceSymbols();
    util::SpuOutfeedHelper outfeed_helper(sctx, device_symbols);
    out = outfeed_helper.DumpPublic(tc.scalar.name);
    // convert hash to string for string tensor in spu
    if (tc.scalar.tensor->Type() == pb::PrimitiveDataType::STRING) {
      out = exec_ctxs[0].GetSession()->HashToString(*out);
    }
  }
  ASSERT_TRUE(out);
  EXPECT_TRUE(out->ToArrowChunkedArray()->ApproxEquals(*expect_arr))
      << "expect type = " << expect_arr->type()->ToString()
      << ", got type = " << out->ToArrowChunkedArray()->type()->ToString()
      << "\nexpect result = " << expect_arr->ToString()
      << "\nbut actual got result = " << out->ToArrowChunkedArray()->ToString();
}

/// ===========================
/// ConstantTest impl
/// ===========================

pb::ExecNode ConstantTest::MakeConstantExecNode(const ConstantTestCase& tc) {
  test::ExecNodeBuilder builder(Constant::kOpType);

  builder.SetNodeName("constant-test");

  auto pb_tensor = std::make_shared<pb::Tensor>();
  pb_tensor->set_elem_type(tc.scalar.tensor->Type());
  util::CopyValuesToProto(tc.scalar.tensor, pb_tensor.get());
  builder.AddAttr(Constant::kScalarAttr, *pb_tensor);

  auto t = test::MakeTensorReference(tc.scalar.name, pb_tensor->elem_type(),
                                     tc.output_status);
  builder.AddOutput(Constant::kOut, {t});

  return builder.Build();
}

}  // namespace scql::engine::op
