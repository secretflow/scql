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

#include "engine/operator/secret_join.h"

#include "arrow/compute/api.h"
#include "libspu/kernel/hal/public_helper.h"
#include "libspu/kernel/hal/shape_ops.h"
#include "libspu/kernel/hal/type_cast.h"
#include "libspu/kernel/hlo/basic_binary.h"
#include "libspu/kernel/hlo/basic_ternary.h"
#include "libspu/kernel/hlo/basic_unary.h"
#include "libspu/kernel/hlo/casting.h"
#include "libspu/kernel/hlo/const.h"
#include "libspu/kernel/hlo/geometrical.h"
#include "libspu/kernel/hlo/indexing.h"
#include "libspu/kernel/hlo/sort.h"

#include "engine/core/tensor_constructor.h"
#include "engine/util/prefix_sum.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

using namespace spu::kernel;

const std::string SecretJoin::kOpType("SecretJoin");

const std::string& SecretJoin::Type() const { return kOpType; }

void SecretJoin::Validate(ExecContext* ctx) {
  auto left_key = ctx->GetInput(kLeftKey);
  YACL_ENFORCE(left_key.size() > 0);
  auto right_key = ctx->GetInput(kRightKey);
  YACL_ENFORCE(left_key.size() == right_key.size(),
               "left key size{} != right key size{}", left_key.size(),
               right_key.size());

  auto left_payload = ctx->GetInput(kLeftPayload);
  YACL_ENFORCE(left_payload.size() > 0);
  auto left_output = ctx->GetOutput(kOutLeft);
  YACL_ENFORCE(left_output.size() == left_payload.size(),
               "left output size{} != left payload size{}", left_output.size(),
               left_payload.size());

  auto right_payload = ctx->GetInput(kRightPayload);
  YACL_ENFORCE(right_payload.size() > 0);
  auto right_output = ctx->GetOutput(kOutRight);
  YACL_ENFORCE(right_output.size() == right_payload.size(),
               "right output size{} != right payload size{}",
               right_output.size(), right_payload.size());

  // check input output are all secret
  YACL_ENFORCE(
      util::AreTensorsStatusMatched(left_key, pb::TENSORSTATUS_SECRET));
  YACL_ENFORCE(
      util::AreTensorsStatusMatched(right_key, pb::TENSORSTATUS_SECRET));
  YACL_ENFORCE(
      util::AreTensorsStatusMatched(left_payload, pb::TENSORSTATUS_SECRET));
  YACL_ENFORCE(
      util::AreTensorsStatusMatched(right_payload, pb::TENSORSTATUS_SECRET));
  YACL_ENFORCE(
      util::AreTensorsStatusMatched(left_output, pb::TENSORSTATUS_SECRET));
  YACL_ENFORCE(
      util::AreTensorsStatusMatched(right_output, pb::TENSORSTATUS_SECRET));
  // TODO: support CHEETAH
  YACL_ENFORCE(ctx->GetSession()->GetSpuContext()->config().protocol() !=
                   spu::ProtocolKind::CHEETAH,
               "sort in CHEETAH is not stable, disable temporaryly");
}

void SecretJoin::Execute(ExecContext* ctx) {
  auto symbols = ctx->GetSession()->GetDeviceSymbols();
  auto sctx = ctx->GetSession()->GetSpuContext();

  auto left_key = symbols->getVar(
      util::SpuVarNameEncoder::GetValueName(ctx->GetInput(kLeftKey)[0].name()));
  auto right_key = symbols->getVar(util::SpuVarNameEncoder::GetValueName(
      ctx->GetInput(kRightKey)[0].name()));
  int64_t left_row_num = left_key.numel();
  int64_t right_row_num = right_key.numel();
  int64_t total_row = left_row_num + right_row_num;

  // 1. Merge left and right table to count key group
  spu::Value zeros = hlo::Constant(sctx, int64_t(0), {left_row_num});
  spu::Value ones = hlo::Constant(sctx, int64_t(1), {right_row_num});
  // [item_origin_mark] using '0' to mark the left rows and '1' to mark the
  // right.
  auto item_origin_mark =
      hlo::Seal(sctx, hlo::Concatenate(sctx, {zeros, ones}, 0));
  const auto [merged_key, merged_payload] = MergeInputs(ctx);
  SPDLOG_INFO("key size:{}, payload size:{}", merged_key.size(),
              merged_payload.size());

  std::vector<spu::Value> sort_inputs = merged_key;
  sort_inputs.push_back(item_origin_mark);
  sort_inputs.insert(sort_inputs.end(), merged_payload.begin(),
                     merged_payload.end());
  // sort_inputs: [keys, item_origin_mark, left_payloads, right_payloads]
  auto results = hlo::SimpleSort(
      sctx, sort_inputs, 0, hal::SortDirection::Ascending,
      merged_key.size() + 1 /* num_keys */, -1 /* valid_bits */);

  // accumulate count of left/right item in key group
  std::vector<spu::Value> sorted_keys(results.begin(),
                                      results.begin() + merged_key.size());
  const auto [left_item_count, right_item_count] =
      CountKeyGroup(sctx, sorted_keys, results[sorted_keys.size()]);

  // 2) Separation: sort by item_origin_mark, then separate left and right to
  // get Ls and Rs
  std::vector<spu::Value> sep_inputs;
  sep_inputs.push_back(results[sorted_keys.size()]);
  sep_inputs.push_back(left_item_count);
  sep_inputs.push_back(right_item_count);
  sep_inputs.insert(sep_inputs.end(), results.begin() + sorted_keys.size() + 1,
                    results.end());
  // inputs: [item_origin_mark, left_item_count, right_item_count,
  // left_payloads, right_payloads]
  // NOTE: set valid_bits to 2 to contain the sign bit, though the
  // item_origin_mark only contains 0/1.
  auto sep_results =
      hlo::SimpleSort(sctx, sep_inputs, 0, hal::SortDirection::Ascending,
                      1 /* num_keys */, 2 /* valid_bits */);

  std::vector<spu::Value> left_payload;
  for (int64_t i = 0; i < ctx->GetInput(kLeftPayload).size(); ++i) {
    left_payload.push_back(
        hlo::Slice(sctx, sep_results[3 + i], {0}, {left_row_num}, {}));
  }

  std::vector<spu::Value> right_payload;
  for (size_t i = 3 + ctx->GetInput(kLeftPayload).size();
       i < sep_results.size(); ++i) {
    right_payload.push_back(
        hlo::Slice(sctx, sep_results[i], {left_row_num}, {total_row}, {}));
  }

  // 3) Expansion
  // reveal left_item_count/right_item_count to generate index to take
  auto public_lc = hal::reveal(sctx, sep_results[1]);
  auto public_rc = hal::reveal(sctx, sep_results[2]);
  spu::NdArrayRef lc_arr = spu::kernel::hal::dump_public(sctx, public_lc);
  spu::NdArrayRef rc_arr = spu::kernel::hal::dump_public(sctx, public_rc);

  spu::Index left_index;
  for (int64_t i = 0; i < left_row_num; ++i) {
    if (lc_arr.at<int64_t>({i}) > 0) {
      for (int64_t j = 0; j < rc_arr.at<int64_t>({i}); ++j) {
        left_index.push_back(i);
      }
    }
    // TODO: support outer join if NULL in secret is supported.
  }
  spu::Index right_index;
  for (int64_t i = 0; i < right_row_num;) {
    if (lc_arr.at<int64_t>({i + left_row_num}) > 0) {
      for (int64_t j = 0; j < lc_arr.at<int64_t>({i + left_row_num}); ++j) {
        for (int64_t k = 0; k < rc_arr.at<int64_t>({i + left_row_num}); ++k) {
          right_index.push_back(i + k);
        }
      }
      i += rc_arr.at<int64_t>({i + left_row_num});
    } else {
      ++i;
    }
  }
  // set result
  auto left_output = ctx->GetOutput(kOutLeft);
  for (int64_t i = 0; i < left_output.size(); ++i) {
    auto v = hlo::LinearGather(sctx, left_payload[i], left_index);
    symbols->setVar(
        util::SpuVarNameEncoder::GetValueName(left_output[i].name()), v);
  }
  auto right_output = ctx->GetOutput(kOutRight);
  for (int64_t i = 0; i < right_output.size(); ++i) {
    auto v = hlo::LinearGather(sctx, right_payload[i], right_index);
    symbols->setVar(
        util::SpuVarNameEncoder::GetValueName(right_output[i].name()), v);
  }
}

// vertically merge inputs from left and right, the keys are concatenated, while
// the payload are filled with dummy items, e.g:
// left table:
//   key   left_data
//    1       2
//
// right table:
//   key   right_data
//    3       4
//
// @returns:
//   key | left_data right data
//    1  |     2       NULL
//    3  |   NULL      4
//
std::tuple<std::vector<spu::Value>, std::vector<spu::Value>>
SecretJoin::MergeInputs(ExecContext* ctx) {
  auto symbols = ctx->GetSession()->GetDeviceSymbols();
  auto sctx = ctx->GetSession()->GetSpuContext();
  // concatenate keys
  int64_t left_row_num = 0;
  int64_t right_row_num = 0;
  std::vector<spu::Value> keys;
  for (int64_t i = 0; i < ctx->GetInput(kLeftKey).size(); ++i) {
    auto left_key = symbols->getVar(util::SpuVarNameEncoder::GetValueName(
        ctx->GetInput(kLeftKey)[i].name()));
    left_row_num = left_key.numel();
    auto right_key = symbols->getVar(util::SpuVarNameEncoder::GetValueName(
        ctx->GetInput(kRightKey)[i].name()));
    right_row_num = right_key.numel();

    auto merged_key = hlo::Concatenate(sctx, {left_key, right_key}, 0);
    keys.push_back(merged_key);
  }

  // fill payloads with dummy items
  auto zeros = hlo::Seal(sctx, hlo::Constant(sctx, int64_t(0), {left_row_num}));
  auto ones = hlo::Seal(sctx, hlo::Constant(sctx, int64_t(1), {right_row_num}));
  std::vector<spu::Value> payloads;
  for (const auto& input : ctx->GetInput(kLeftPayload)) {
    auto left_value =
        symbols->getVar(util::SpuVarNameEncoder::GetValueName(input.name()));
    // add dummy items to the end of left value
    auto tmp = hlo::Concatenate(
        sctx, {left_value, hal::dtype_cast(sctx, ones, left_value.dtype())}, 0);
    payloads.push_back(tmp);
  }
  for (const auto& input : ctx->GetInput(kRightPayload)) {
    auto right_value =
        symbols->getVar(util::SpuVarNameEncoder::GetValueName(input.name()));
    // add dummy items to the begin of right value
    auto tmp = hlo::Concatenate(
        sctx, {hal::dtype_cast(sctx, zeros, right_value.dtype()), right_value},
        0);
    payloads.push_back(tmp);
  }

  return std::make_pair(keys, payloads);
}

// count the number of left and right items in each key group
// @param[keys] are the sorted keys merged from left and right table.
// @param[item_origin_mark] using '0' to mark the items in left table and '1' to
// mark the right. e.g:
// keys, item_origin_mark
//   1,              0
//   1,              1
//   1,              1
//   2,              1
//
// @returns
// left_count, right_count
//          1,           2
//          1,           2
//          1,           2
//          0,           1
std::tuple<spu::Value, spu::Value> SecretJoin::CountKeyGroup(
    spu::SPUContext* sctx, const std::vector<spu::Value>& keys,
    const spu::Value& item_origin_mark) {
  // 1. calculate key group mark, where '0' mark the begining of key group
  auto total_row = keys[0].numel();
  auto key_equal =
      hlo::Equal(sctx, hlo::Slice(sctx, keys[0], {0}, {total_row - 1}, {}),
                 hlo::Slice(sctx, keys[0], {1}, {total_row}, {}));
  for (size_t i = 1; i < keys.size(); ++i) {
    auto key_equal_i =
        hlo::Equal(sctx, hlo::Slice(sctx, keys[i], {0}, {total_row - 1}, {}),
                   hlo::Slice(sctx, keys[i], {1}, {total_row}, {}));
    key_equal = hlo::And(sctx, key_equal, key_equal_i);
  }
  auto zero = hlo::Seal(sctx, hlo::Constant(sctx, false, {1}));
  auto key_group_mark =
      hlo::Concatenate(sctx, std::vector<spu::Value>{zero, key_equal}, 0);
  auto shifted_mark =
      hlo::Concatenate(sctx, std::vector<spu::Value>{key_equal, zero}, 0);

  // 2. accumulate count of left/right item in key group
  auto tmp_right_count = util::Scan(
      sctx, item_origin_mark, key_group_mark,
      [&](const spu::Value& lhs_v, const spu::Value& lhs_gm,
          const spu::Value& rhs_v, const spu::Value& rhs_gm) {
        spu::Value new_v = hlo::Add(sctx, lhs_v, hlo::Mul(sctx, lhs_gm, rhs_v));
        spu::Value new_gm = hlo::Mul(sctx, lhs_gm, rhs_gm);

        return std::make_pair(new_v, new_gm);
      });
  auto flip_mark =
      hlo::Sub(sctx, hlo::Seal(sctx, hlo::Constant(sctx, 1, {total_row})),
               item_origin_mark);
  auto tmp_left_count = util::Scan(
      sctx, flip_mark, key_group_mark,
      [&](const spu::Value& lhs_v, const spu::Value& lhs_gm,
          const spu::Value& rhs_v, const spu::Value& rhs_gm) {
        spu::Value new_v = hlo::Add(sctx, lhs_v, hlo::Mul(sctx, lhs_gm, rhs_v));
        spu::Value new_gm = hlo::Mul(sctx, lhs_gm, rhs_gm);

        return std::make_pair(new_v, new_gm);
      });

  // 3. reverse propagation left/right count
  auto reversed_left_count = util::Scan(
      sctx, hlo::Reverse(sctx, tmp_left_count, {0}),
      hlo::Reverse(sctx, shifted_mark, {0}),
      [&](const spu::Value& lhs_v, const spu::Value& lhs_gm,
          const spu::Value& rhs_v, const spu::Value& rhs_gm) {
        spu::Value new_v = hlo::Add(
            sctx, lhs_v, hlo::Mul(sctx, lhs_gm, hlo::Sub(sctx, rhs_v, lhs_v)));
        spu::Value new_gm = hlo::Mul(sctx, lhs_gm, rhs_gm);

        return std::make_pair(new_v, new_gm);
      });
  auto reversed_right_count = util::Scan(
      sctx, hlo::Reverse(sctx, tmp_right_count, {0}),
      hlo::Reverse(sctx, shifted_mark, {0}),
      [&](const spu::Value& lhs_v, const spu::Value& lhs_gm,
          const spu::Value& rhs_v, const spu::Value& rhs_gm) {
        spu::Value new_v = hlo::Add(
            sctx, lhs_v, hlo::Mul(sctx, lhs_gm, hlo::Sub(sctx, rhs_v, lhs_v)));
        spu::Value new_gm = hlo::Mul(sctx, lhs_gm, rhs_gm);

        return std::make_pair(new_v, new_gm);
      });

  return std::make_pair(hlo::Reverse(sctx, reversed_left_count, {0}),
                        hlo::Reverse(sctx, reversed_right_count, {0}));
}

}  // namespace scql::engine::op