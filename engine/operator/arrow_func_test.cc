// Copyright 2024 Ant Group Co., Ltd.
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

#include "engine/operator/arrow_func.h"

#include "absl/strings/escaping.h"
#include "arrow/compute/api.h"
#include "gtest/gtest.h"

#include "engine/core/arrow_helper.h"
#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"

namespace scql::engine::op {

struct ArrowFuncTestCase {
  std::vector<test::NamedTensor> ins;
  std::string func_name;
  std::shared_ptr<arrow::compute::FunctionOptions> func_opt;
  std::vector<test::NamedTensor> expect_outs;
};

class ArrowFuncTest
    : public testing::TestWithParam<
          std::tuple<test::SpuRuntimeTestCase, ArrowFuncTestCase>> {
 protected:
  static pb::ExecNode MakeExecNode(const ArrowFuncTestCase& tc);
  static void FeedInputs(ExecContext* ctx, const ArrowFuncTestCase& tc);
};

INSTANTIATE_TEST_SUITE_P(
    ArrowFuncBatchTest, ArrowFuncTest,
    testing::Combine(
        testing::Values(test::SpuRuntimeTestCase{spu::ProtocolKind::SEMI2K, 2}),
        testing::Values(
            ArrowFuncTestCase{
                .ins = {test::NamedTensor(
                    "in0", TensorFrom(arrow::large_utf8(),
                                     R"json(["A", "B", ""])json")),test::NamedTensor(
                    "in1", TensorFrom(arrow::large_utf8(),
                                     R"json(["", "2", "3"])json")),test::NamedTensor(
                    "sep", TensorFrom(arrow::large_utf8(),
                                     R"json(["*", "?", ""])json"))},
                .func_name = "binary_join_element_wise",
                .expect_outs = {test::NamedTensor(
                    "out", TensorFrom(arrow::large_utf8(),
                                      R"json(["A*", "B?2", "3"])json"))}},
            ArrowFuncTestCase{
                .ins = {test::NamedTensor(
                    "in_date_str", TensorFrom(arrow::large_utf8(),
                                     R"json(["2024-06-18", null, "2025-01-01", "invalid-date"])json"))},
                .func_name = "strptime",
                .func_opt = std::make_shared<arrow::compute::StrptimeOptions>(
                    "%Y-%m-%d",
                    arrow::TimeUnit::SECOND,
                    true),
                .expect_outs = {test::NamedTensor(
                    "out",
                    TensorFrom(arrow::int64(),
                               R"json([1718668800, null, 1735689600, null])json"))}},
            ArrowFuncTestCase{
                .ins = {test::NamedTensor(
                    "in", TensorFrom(arrow::large_utf8(),
                                     R"json(["alice  ", "  Bob", " CAROL ",
                        " "])json"))},
                .func_name = "utf8_trim",
                .func_opt = std::make_shared<arrow::compute::TrimOptions>(" "),
                .expect_outs = {test::NamedTensor(
                    "out", TensorFrom(arrow::large_utf8(),
                                      R"json(["alice", "Bob", "CAROL",
                        ""])json"))}},
            ArrowFuncTestCase{
                .ins = {test::NamedTensor(
                    "in", TensorFrom(arrow::large_utf8(),
                                     R"json(["alice", "Bob", "CAROL",
                        "AA#Bb.cc"])json"))},
                .func_name = "utf8_slice_codeunits",
                .func_opt = std::make_shared<arrow::compute::SliceOptions>(4 /* start */, 10 /* stop */,
                                           1 /* step */),
                .expect_outs = {test::NamedTensor(
                    "out", TensorFrom(arrow::large_utf8(),
                                      R"json(["e", "", "L",
                        "b.cc"])json"))}},
            ArrowFuncTestCase{
                .ins = {test::NamedTensor(
                    "in",
                    TensorFrom(
                        arrow::large_utf8(),
                        R"json(["alice", "Bob", "CAROL", "AA#Bb.cc"])json"))},
                .func_name = "utf8_lower",
                .expect_outs = {test::NamedTensor(
                    "out",
                    TensorFrom(
                        arrow::large_utf8(),
                        R"json(["alice", "bob", "carol", "aa#bb.cc"])json"))}},
            ArrowFuncTestCase{
                .ins = {test::NamedTensor(
                    "in",
                    TensorFrom(
                        arrow::large_utf8(),
                        R"json(["alice", "Bob", "CAROL", "AA#Bb.cc"])json"))},
                .func_name = "utf8_upper",
                .expect_outs = {test::NamedTensor(
                    "out",
                    TensorFrom(
                        arrow::large_utf8(),
                        R"json(["ALICE", "BOB", "CAROL", "AA#BB.CC"])json"))}},
            ArrowFuncTestCase{
                .ins = {test::NamedTensor("in0", TensorFrom(arrow::float64(),
                                                            "[null, 2.4]")),
                        test::NamedTensor("in1", TensorFrom(arrow::float64(),
                                                            "[2.4, 100100]"))},
                .func_name = "multiply",
                .expect_outs = {test::NamedTensor(
                    "out", TensorFrom(arrow::float64(), "[null, 240240]"))}},
            ArrowFuncTestCase{
                .ins = {test::NamedTensor(
                    "in",
                    TensorFrom(arrow::float64(), "[1.3, null, 2.4, null]"))},
                .func_name = "is_null",
                .expect_outs = {test::NamedTensor(
                    "out", TensorFrom(arrow::boolean(), "[0, 1, 0, 1]"))}})),
    TestParamNameGenerator(ArrowFuncTest));

TEST_P(ArrowFuncTest, works) {
  // Given
  auto parm = GetParam();
  auto tc = std::get<1>(parm);
  auto node = MakeExecNode(tc);
  auto session = test::Make1PCSession();
  ExecContext ctx(node, session.get());

  FeedInputs(&ctx, tc);

  // When
  ArrowFunc op;
  ASSERT_NO_THROW(op.Run(&ctx));

  // Then
  // check alice output
  for (auto& out : tc.expect_outs) {
    TensorPtr t = session->GetTensorTable()->GetTensor(out.name);
    ASSERT_TRUE(t);
    auto out_arr = t->ToArrowChunkedArray();

    auto expect_arr = out.tensor->ToArrowChunkedArray();

    // compare tensor content
    EXPECT_TRUE(out_arr->Equals(expect_arr))
        << "expect type = " << expect_arr->type()->ToString()
        << ", got type = " << out_arr->type()->ToString()
        << "\nexpect result = " << expect_arr->ToString()
        << "\nbut actual got result = " << out_arr->ToString();
  }
}

/// ===================
/// ArrowFuncTest impl
/// ===================

pb::ExecNode ArrowFuncTest::MakeExecNode(const ArrowFuncTestCase& tc) {
  test::ExecNodeBuilder builder(ArrowFunc::kOpType);
  builder.SetNodeName("arrow-func-test");
  builder.AddStringAttr(ArrowFunc::kFuncNameAttr, tc.func_name);
  if (tc.func_opt != nullptr) {
    builder.AddStringAttr(ArrowFunc::kFuncOptTypeAttr,
                          tc.func_opt->type_name());
    std::shared_ptr<arrow::Buffer> buf;
    ASSIGN_OR_THROW_ARROW_STATUS(buf, tc.func_opt->Serialize());
    builder.AddStringAttr(ArrowFunc::kFuncOptionsAttr,
                          absl::Base64Escape(buf->ToString()));
  }

  std::vector<pb::Tensor> inputs;
  for (auto& in : tc.ins) {
    auto t = test::MakePrivateTensorReference(in.name, in.tensor->Type());
    inputs.push_back(t);
  }
  builder.AddInput(ArrowFunc::kIn, inputs);

  std::vector<pb::Tensor> outputs;
  for (auto& out : tc.expect_outs) {
    auto t = test::MakePrivateTensorReference(out.name, out.tensor->Type());
    outputs.push_back(t);
  }
  builder.AddOutput(ArrowFunc::kOut, outputs);

  return builder.Build();
}

void ArrowFuncTest::FeedInputs(ExecContext* ctx, const ArrowFuncTestCase& tc) {
  test::FeedInputsAsPrivate(ctx, tc.ins);
}

}  // namespace scql::engine::op