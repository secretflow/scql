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

#include "engine/operator/join.h"

#include <cstddef>
#include <cstdint>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "engine/core/primitive_builder.h"
#include "engine/core/tensor_constructor.h"
#include "engine/operator/test_util.h"
#include "engine/util/psi_helper.h"

namespace scql::engine::op {

struct JoinTestCase {
  std::vector<test::NamedTensor> left_inputs;
  std::vector<test::NamedTensor> right_inputs;
  int64_t join_type = 0;
  int64_t join_algo = 0;
  // join indices in following format:
  // ["left_idx1,right_idx1", "left_idx2,right_idx2",...]
  std::vector<std::string> join_indices;
  int64_t ub_server;
};

class JoinTest : public ::testing::TestWithParam<
                     std::tuple<test::SpuRuntimeTestCase, JoinTestCase>> {
 protected:
  static pb::ExecNode MakeJoinExecNode(const JoinTestCase& tc);

  static void FeedInputs(ExecContext* ctx,
                         const std::vector<test::NamedTensor>& tensors);

  static constexpr char kOutLeft[] = "left_output";
  static constexpr char kOutRight[] = "right_output";
};

namespace {
std::vector<std::string> AllMatchedJoinIndices(size_t left_count,
                                               size_t right_count) {
  std::vector<std::string> ret;
  for (size_t left_idx = 0; left_idx < left_count; ++left_idx) {
    for (size_t right_idx = 0; right_idx < right_count; ++right_idx) {
      ret.emplace_back(absl::StrCat(left_idx, ",", right_idx));
    }
  }
  return ret;
}
}  // namespace

INSTANTIATE_TEST_SUITE_P(
    JoinBatchTest, JoinTest,
    testing::Combine(
        test::SpuTestValues2PC,
        testing::Values(
            // ECDH PSI

            // case 1
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "id_a", TensorFrom(arrow::int64(), "[4,8,1,3,3,5]"))},
                .right_inputs = {test::NamedTensor(
                    "id_b", TensorFrom(arrow::int64(), "[2,1,4,4,3,7,8]"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kInnerJoin),
                .join_indices = {"0,2", "0,3", "1,6", "2,1", "3,4", "4,4"}},

            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "id_a", TensorFrom(arrow::int64(), "[4,8,1,3,3,5]"))},
                .right_inputs = {test::NamedTensor(
                    "id_b", TensorFrom(arrow::int64(), "[2,1,4,4,3,7,8]"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kLeftJoin),
                .join_indices = {"0,2", "0,3", "1,6", "2,1", "3,4", "4,4",
                                 "5,null"}},
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "id_a", TensorFrom(arrow::int64(), "[4,8,1,3,3,5]"))},
                .right_inputs = {test::NamedTensor(
                    "id_b", TensorFrom(arrow::int64(), "[2,1,4,4,3,7,8]"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kRightJoin),
                .join_indices = {"0,2", "0,3", "1,6", "2,1", "3,4", "4,4",
                                 "null,0", "null,5"}},
            // clang-format off

        //   ta inner join tb on x_1=y_1 and x_2 = y_2
        //   join table is:
        //         ta_index, x_1, x_2,    tb_index, y_1, y_2
        //                0,   A,   1,           1,   A, 1
        //                1,   A,   2,           2,   A, 2
        //                2,   B,   1,           3,   B, 1
        //                3,   B,   1,           3,   B, 1
        //                4,   B,   2,           0,   B, 2
        //                4,   B,   2,           5,   B, 2

        //   ta left join tb on x_1=y_1 and x_2 = y_2
        //   join table is:
        //         ta_index, x_1,  x_2,    b_index,  y_1,   y_2
        //                0,   A,   1,           1,    A,    1
        //                1,   A,   2,           2,    A,    2
        //                2,   B,   1,           3,    B,    1
        //                3,   B,   1,           3,    B,    1
        //                4,   B,   2,           0,    B,    2
        //                4,   B,   2,           5,    B,    2
        //                5,   C,   1,           null, null, null

        //   ta right join tb on x_1=y_1 and x_2 = y_2
        //   join table is:
        //         ta_index,  x_1,   x_2,    b_index,  y_1,   y_2
        //                0,    A,    1,           1,    A,    1
        //                1,    A,    2,           2,    A,    2
        //                2,    B,    1,           3,    B,    1
        //                3,    B,    1,           3,    B,    1
        //                4,    B,    2,           0,    B,    2
        //                4,    B,    2,           5,    B,    2
        //                null, null, null,        4,    B,    3
            // clang-format on
            // auto tensorX_1 = test::NamedTensor(
            //     "x_1", TensorFrom(arrow::large_utf8(),
            // R"json(["A","A","B","B","B","C"])json"));

            // case 2
            JoinTestCase{
                .left_inputs =
                    {test::NamedTensor(
                         "x_1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["A","A","B","B","B","C"])json")),
                     test::NamedTensor("x_2", TensorFrom(arrow::int64(),
                                                         "[1,2,1,1,2,1]"))},
                .right_inputs =
                    {test::NamedTensor(
                         "y_1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["B","A","A","B","B","B"])json")),
                     test::NamedTensor("y_2", TensorFrom(arrow::int64(),
                                                         "[2,1,2,1,3,2]"))},
                .join_indices = {"0,1", "1,2", "2,3", "3,3", "4,0", "4,5"}},

            JoinTestCase{
                .left_inputs =
                    {test::NamedTensor(
                         "x_1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["A","A","B","B","B","C"])json")),
                     test::NamedTensor("x_2", TensorFrom(arrow::int64(),
                                                         "[1,2,1,1,2,1]"))},
                .right_inputs =
                    {test::NamedTensor(
                         "y_1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["B","A","A","B","B","B"])json")),
                     test::NamedTensor("y_2", TensorFrom(arrow::int64(),
                                                         "[2,1,2,1,3,2]"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kLeftJoin),
                .join_indices = {"0,1", "1,2", "2,3", "3,3", "4,0", "4,5",
                                 "5,null"}},
            JoinTestCase{
                .left_inputs =
                    {test::NamedTensor(
                         "x_1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["A","A","B","B","B","C"])json")),
                     test::NamedTensor("x_2", TensorFrom(arrow::int64(),
                                                         "[1,2,1,1,2,1]"))},
                .right_inputs =
                    {test::NamedTensor(
                         "y_1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["B","A","A","B","B","B"])json")),
                     test::NamedTensor("y_2", TensorFrom(arrow::int64(),
                                                         "[2,1,2,1,3,2]"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kRightJoin),
                .join_indices = {"0,1", "1,2", "2,3", "3,3", "4,0", "4,5",
                                 "null,4"}},

            // case 3
            // testcase: empty join output
            JoinTestCase{.left_inputs = {test::NamedTensor(
                             "x", TensorFrom(arrow::large_utf8(),
                                             R"json(["A","B","C"])json"))},
                         .right_inputs = {test::NamedTensor(
                             "y", TensorFrom(arrow::large_utf8(),
                                             R"json(["E","F","H","G"])json"))},
                         .join_indices = {}},
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(),
                                    R"json(["A","B","C"])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["E","F","H","G"])json"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kLeftJoin),
                .join_indices = {"0,null", "1,null", "2,null"}},
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(),
                                    R"json(["A","B","C"])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["E","F","H","G"])json"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kRightJoin),
                .join_indices = {"null,0", "null,1", "null,2", "null,3"}},

            // case 4
            // testcase: empty input
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(), R"json([])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["E","F","H","G"])json"))},
                .join_indices = {}},
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(), R"json([])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["E","F","H","G"])json"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kLeftJoin),
                .join_indices = {}},
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(), R"json([])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["E","F","H","G"])json"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kRightJoin),
                .join_indices = {"null,0", "null,1", "null,2", "null,3"}},

            // UbPsiJoin

            // with hint

            // case 1
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "id_a", TensorFrom(arrow::int64(), "[4,8,1,3,3,5]"))},
                .right_inputs = {test::NamedTensor(
                    "id_b", TensorFrom(arrow::int64(), "[2,1,4,4,3,7,8]"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kInnerJoin),
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {"0,2", "0,3", "1,6", "2,1", "3,4", "4,4"},
                .ub_server = 0},

            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "id_a", TensorFrom(arrow::int64(), "[4,8,1,3,3,5]"))},
                .right_inputs = {test::NamedTensor(
                    "id_b", TensorFrom(arrow::int64(), "[2,1,4,4,3,7,8]"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kLeftJoin),
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {"0,2", "0,3", "1,6", "2,1", "3,4", "4,4",
                                 "5,null"},
                .ub_server = 0},
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "id_a", TensorFrom(arrow::int64(), "[4,8,1,3,3,5]"))},
                .right_inputs = {test::NamedTensor(
                    "id_b", TensorFrom(arrow::int64(), "[2,1,4,4,3,7,8]"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kRightJoin),
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {"0,2", "0,3", "1,6", "2,1", "3,4", "4,4",
                                 "null,0", "null,5"},
                .ub_server = 0},
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "id_a", TensorFrom(arrow::int64(), "[4,8,1,3,3,5]"))},
                .right_inputs = {test::NamedTensor(
                    "id_b", TensorFrom(arrow::int64(), "[2,1,4,4,3,7,8]"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kInnerJoin),
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {"0,2", "0,3", "1,6", "2,1", "3,4", "4,4"},
                .ub_server = 1},

            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "id_a", TensorFrom(arrow::int64(), "[4,8,1,3,3,5]"))},
                .right_inputs = {test::NamedTensor(
                    "id_b", TensorFrom(arrow::int64(), "[2,1,4,4,3,7,8]"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kLeftJoin),
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {"0,2", "0,3", "1,6", "2,1", "3,4", "4,4",
                                 "5,null"},
                .ub_server = 1},
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "id_a", TensorFrom(arrow::int64(), "[4,8,1,3,3,5]"))},
                .right_inputs = {test::NamedTensor(
                    "id_b", TensorFrom(arrow::int64(), "[2,1,4,4,3,7,8]"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kRightJoin),
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {"0,2", "0,3", "1,6", "2,1", "3,4", "4,4",
                                 "null,0", "null,5"},
                .ub_server = 1},

            // case 2
            JoinTestCase{
                .left_inputs =
                    {test::NamedTensor(
                         "x_1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["A","A","B","B","B","C"])json")),
                     test::NamedTensor("x_2", TensorFrom(arrow::int64(),
                                                         "[1,2,1,1,2,1]"))},
                .right_inputs =
                    {test::NamedTensor(
                         "y_1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["B","A","A","B","B","B"])json")),
                     test::NamedTensor("y_2", TensorFrom(arrow::int64(),
                                                         "[2,1,2,1,3,2]"))},
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {"0,1", "1,2", "2,3", "3,3", "4,0", "4,5"},
                .ub_server = 0},

            JoinTestCase{
                .left_inputs =
                    {test::NamedTensor(
                         "x_1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["A","A","B","B","B","C"])json")),
                     test::NamedTensor("x_2", TensorFrom(arrow::int64(),
                                                         "[1,2,1,1,2,1]"))},
                .right_inputs =
                    {test::NamedTensor(
                         "y_1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["B","A","A","B","B","B"])json")),
                     test::NamedTensor("y_2", TensorFrom(arrow::int64(),
                                                         "[2,1,2,1,3,2]"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kLeftJoin),
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {"0,1", "1,2", "2,3", "3,3", "4,0", "4,5",
                                 "5,null"},
                .ub_server = 0},
            JoinTestCase{
                .left_inputs =
                    {test::NamedTensor(
                         "x_1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["A","A","B","B","B","C"])json")),
                     test::NamedTensor("x_2", TensorFrom(arrow::int64(),
                                                         "[1,2,1,1,2,1]"))},
                .right_inputs =
                    {test::NamedTensor(
                         "y_1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["B","A","A","B","B","B"])json")),
                     test::NamedTensor("y_2", TensorFrom(arrow::int64(),
                                                         "[2,1,2,1,3,2]"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kRightJoin),
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {"0,1", "1,2", "2,3", "3,3", "4,0", "4,5",
                                 "null,4"},
                .ub_server = 0},

            JoinTestCase{
                .left_inputs =
                    {test::NamedTensor(
                         "x_1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["A","A","B","B","B","C"])json")),
                     test::NamedTensor("x_2", TensorFrom(arrow::int64(),
                                                         "[1,2,1,1,2,1]"))},
                .right_inputs =
                    {test::NamedTensor(
                         "y_1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["B","A","A","B","B","B"])json")),
                     test::NamedTensor("y_2", TensorFrom(arrow::int64(),
                                                         "[2,1,2,1,3,2]"))},
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {"0,1", "1,2", "2,3", "3,3", "4,0", "4,5"},
                .ub_server = 1},

            JoinTestCase{
                .left_inputs =
                    {test::NamedTensor(
                         "x_1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["A","A","B","B","B","C"])json")),
                     test::NamedTensor("x_2", TensorFrom(arrow::int64(),
                                                         "[1,2,1,1,2,1]"))},
                .right_inputs =
                    {test::NamedTensor(
                         "y_1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["B","A","A","B","B","B"])json")),
                     test::NamedTensor("y_2", TensorFrom(arrow::int64(),
                                                         "[2,1,2,1,3,2]"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kLeftJoin),
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {"0,1", "1,2", "2,3", "3,3", "4,0", "4,5",
                                 "5,null"},
                .ub_server = 1},
            JoinTestCase{
                .left_inputs =
                    {test::NamedTensor(
                         "x_1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["A","A","B","B","B","C"])json")),
                     test::NamedTensor("x_2", TensorFrom(arrow::int64(),
                                                         "[1,2,1,1,2,1]"))},
                .right_inputs =
                    {test::NamedTensor(
                         "y_1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["B","A","A","B","B","B"])json")),
                     test::NamedTensor("y_2", TensorFrom(arrow::int64(),
                                                         "[2,1,2,1,3,2]"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kRightJoin),
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {"0,1", "1,2", "2,3", "3,3", "4,0", "4,5",
                                 "null,4"},
                .ub_server = 1},

            // case 3
            // testcase: empty join output
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(),
                                    R"json(["A","B","C"])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["E","F","H","G"])json"))},
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {},
                .ub_server = 0},
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(),
                                    R"json(["A","B","C"])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["E","F","H","G"])json"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kLeftJoin),
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {"0,null", "1,null", "2,null"},
                .ub_server = 0},
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(),
                                    R"json(["A","B","C"])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["E","F","H","G"])json"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kRightJoin),
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {"null,0", "null,1", "null,2", "null,3"},
                .ub_server = 0},
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(),
                                    R"json(["A","B","C"])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["E","F","H","G"])json"))},
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {},
                .ub_server = 1},
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(),
                                    R"json(["A","B","C"])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["E","F","H","G"])json"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kLeftJoin),
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {"0,null", "1,null", "2,null"},
                .ub_server = 1},
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(),
                                    R"json(["A","B","C"])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["E","F","H","G"])json"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kRightJoin),
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {"null,0", "null,1", "null,2", "null,3"},
                .ub_server = 1},

            // case 4
            // testcase: empty input
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(), R"json([])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["E","F","H","G"])json"))},
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {},
                .ub_server = 0},
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(), R"json([])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["E","F","H","G"])json"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kLeftJoin),
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {},
                .ub_server = 0},
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(), R"json([])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["E","F","H","G"])json"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kRightJoin),
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {"null,0", "null,1", "null,2", "null,3"},
                .ub_server = 0},
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(), R"json([])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["E","F","H","G"])json"))},
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {},
                .ub_server = 1},
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(), R"json([])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["E","F","H","G"])json"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kLeftJoin),
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {},
                .ub_server = 1},
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(), R"json([])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["E","F","H","G"])json"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kRightJoin),
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = {"null,0", "null,1", "null,2", "null,3"},
                .ub_server = 1},

            // case 5
            // testcase: duplicated keys
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "id_a", FullNumericTensor<arrow::Int64Type>(5, 1))},
                .right_inputs = {test::NamedTensor(
                    "id_b", FullNumericTensor<arrow::Int64Type>(1000, 1))},
                .join_type = static_cast<int64_t>(Join::JoinType::kInnerJoin),
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kOprfPsi),
                .join_indices = AllMatchedJoinIndices(5, 1000),
                .ub_server = 1},

            // without hint

            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "id_a", TensorFrom(arrow::int64(), "[4,8,1,3,3,5]"))},
                .right_inputs = {test::NamedTensor(
                    "id_b", TensorFrom(arrow::int64(), "[2,1,4,4,3,7,8]"))},
                .join_type = static_cast<int64_t>(Join::JoinType::kInnerJoin),
                .join_indices = {"0,2", "0,3", "1,6", "2,1", "3,4", "4,4"},
                .ub_server = 1,
            },

            // case 2
            JoinTestCase{
                .left_inputs =
                    {test::NamedTensor(
                         "x_1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["A","A","B","B","B","C"])json")),
                     test::NamedTensor("x_2", TensorFrom(arrow::int64(),
                                                         "[1,2,1,1,2,1]"))},
                .right_inputs =
                    {test::NamedTensor(
                         "y_1",
                         TensorFrom(arrow::large_utf8(),
                                    R"json(["B","A","A","B","B","B"])json")),
                     test::NamedTensor("y_2", TensorFrom(arrow::int64(),
                                                         "[2,1,2,1,3,2]"))},
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kAutoPsi),
                .join_indices = {"0,1", "1,2", "2,3", "3,3", "4,0", "4,5"},
                .ub_server = 1,
            },

            // case 3
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(),
                                    R"json(["A","B","C"])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["E","F","H","G"])json"))},
                .join_algo = static_cast<int64_t>(util::PsiAlgo::kEcdhPsi),
                .join_indices = {},
                .ub_server = 1,
            },

            // case 4
            JoinTestCase{
                .left_inputs = {test::NamedTensor(
                    "x", TensorFrom(arrow::large_utf8(), R"json([])json"))},
                .right_inputs = {test::NamedTensor(
                    "y", TensorFrom(arrow::large_utf8(),
                                    R"json(["E","F","H","G"])json"))},
                .join_indices = {},
                .ub_server = 1,
            }

            )),

    TestParamNameGenerator(JoinTest));

TEST_P(JoinTest, works) {
  // Given
  auto parm = GetParam();
  auto test_case = std::get<1>(parm);
  pb::ExecNode node = MakeJoinExecNode(test_case);
  auto sessions = test::MakeMultiPCSession(std::get<0>(parm));

  ExecContext alice_ctx(node, sessions[0].get());
  ExecContext bob_ctx(node, sessions[1].get());

  // feed inputs
  FeedInputs(&alice_ctx, test_case.left_inputs);
  FeedInputs(&bob_ctx, test_case.right_inputs);

  // When
  EXPECT_NO_THROW(test::RunAsync<Join>({&alice_ctx, &bob_ctx}));

  // Then
  // left output
  auto left_output = alice_ctx.GetTensorTable()->GetTensor(kOutLeft);
  EXPECT_TRUE(left_output != nullptr);
  EXPECT_EQ(left_output->Type(), pb::PrimitiveDataType::INT64);

  // right output
  auto right_output = bob_ctx.GetTensorTable()->GetTensor(kOutRight);
  EXPECT_TRUE(right_output != nullptr);
  EXPECT_EQ(right_output->Type(), pb::PrimitiveDataType::INT64);

  EXPECT_EQ(left_output->Length(), right_output->Length());
  EXPECT_EQ(left_output->Length(), test_case.join_indices.size());

  auto left_join_result =
      util::StringifyVisitor(left_output, left_output->Length())
          .StringifyBatch();
  auto right_join_result =
      util::StringifyVisitor(right_output, right_output->Length())
          .StringifyBatch();

  auto indices_got = util::Combine(left_join_result, right_join_result);

  EXPECT_THAT(indices_got,
              ::testing::UnorderedElementsAreArray(test_case.join_indices));
}

/// ===========================
/// JoinTest impl
/// ===========================

pb::ExecNode JoinTest::MakeJoinExecNode(const JoinTestCase& tc) {
  test::ExecNodeBuilder builder(Join::kOpType);

  builder.SetNodeName("join-test");
  builder.AddInt64Attr(Join::kJoinTypeAttr, tc.join_type);
  builder.AddInt64Attr(Join::kAlgorithmAttr, tc.join_algo);
  builder.AddInt64Attr(Join::kUbPsiServerHint, tc.ub_server);
  builder.AddStringsAttr(
      Join::kInputPartyCodesAttr,
      std::vector<std::string>{test::kPartyAlice, test::kPartyBob});

  // Add inputs

  std::vector<pb::Tensor> left_inputs;
  for (const auto& named_tensor : tc.left_inputs) {
    auto t = test::MakeTensorReference(named_tensor.name,
                                       named_tensor.tensor->Type(),
                                       pb::TensorStatus::TENSORSTATUS_PRIVATE);
    left_inputs.push_back(std::move(t));
  }
  builder.AddInput(Join::kInLeft, left_inputs);

  std::vector<pb::Tensor> right_inputs;
  for (const auto& named_tensor : tc.right_inputs) {
    auto t = test::MakeTensorReference(named_tensor.name,
                                       named_tensor.tensor->Type(),
                                       pb::TensorStatus::TENSORSTATUS_PRIVATE);
    right_inputs.push_back(std::move(t));
  }
  builder.AddInput(Join::kInRight, right_inputs);

  // Add outputs
  auto left_join_output =
      test::MakeTensorReference(kOutLeft, pb::PrimitiveDataType::INT64,
                                pb::TensorStatus::TENSORSTATUS_PRIVATE);
  auto right_join_output =
      test::MakeTensorReference(kOutRight, pb::PrimitiveDataType::INT64,
                                pb::TensorStatus::TENSORSTATUS_PRIVATE);
  builder.AddOutput(Join::kOutLeftJoinIndex,
                    std::vector<pb::Tensor>{left_join_output});
  builder.AddOutput(Join::kOutRightJoinIndex,
                    std::vector<pb::Tensor>{right_join_output});

  return builder.Build();
}

void JoinTest::FeedInputs(ExecContext* ctx,
                          const std::vector<test::NamedTensor>& tensors) {
  auto tensor_table = ctx->GetTensorTable();
  for (const auto& named_tensor : tensors) {
    tensor_table->AddTensor(named_tensor.name, named_tensor.tensor);
  }
}

}  // namespace scql::engine::op