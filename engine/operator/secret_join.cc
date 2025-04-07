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
#include "libspu/kernel/hal/debug.h"
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
#include "libspu/kernel/hlo/permute.h"
#include "libspu/kernel/hlo/shuffle.h"
#include "libspu/kernel/hlo/soprf.h"
#include "libspu/kernel/hlo/sort.h"

#include "engine/core/tensor_constructor.h"
#include "engine/util/prefix_sum.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

using namespace spu::kernel;

namespace {

std::vector<spu::Value> FillDummyValue(spu::SPUContext* sctx,
                                       const std::vector<spu::Value>& inputs,
                                       int64_t dummy_size) {
  if (dummy_size <= 0) {
    return inputs;
  }
  auto zeros = hlo::Seal(sctx, hlo::Constant(sctx, int64_t{0}, {dummy_size}));
  std::vector<spu::Value> ret;
  ret.reserve(inputs.size());
  for (auto v : inputs) {
    ret.push_back(hlo::Concatenate(
        sctx, {v, hal::dtype_cast(sctx, zeros, v.dtype())}, 0));
  }
  return ret;
}

// return the join result position and the upper bound of join size
std::tuple<spu::Value, spu::Value> CountPosition(spu::SPUContext* sctx,
                                                 const spu::Value& count,
                                                 const spu::Value& mark) {
  auto one = hlo::Seal(sctx, hlo::Constant(sctx, int64_t{1}, {1}));
  auto c = hlo::Seal(sctx, hlo::Constant(sctx, int64_t{0}, {1}));
  std::vector<spu::Value> tmp_a;
  for (int64_t i = 0; i < count.numel(); ++i) {
    c = hlo::Add(sctx, c, hlo::Slice(sctx, count, {i}, {i + 1}, {}));
    tmp_a.push_back(c);
  }

  std::vector<spu::Value> tmp_b;
  for (int64_t i = 0; i < mark.numel(); ++i) {
    c = hlo::Sub(sctx, hlo::Add(sctx, c, one),
                 hlo::Slice(sctx, mark, {i}, {i + 1}, {}));
    tmp_b.push_back(c);
  }

  auto tmp_bc = hlo::Concatenate(sctx, tmp_b, 0);
  auto p = hlo::Add(
      sctx,
      hlo::Mul(sctx, mark,
               hlo::Sub(sctx, hlo::Concatenate(sctx, tmp_a, 0), tmp_bc)),
      tmp_bc);
  // index start from 0
  auto index = hlo::Sub(sctx, p, hlo::Constant(sctx, int64_t{1}, {p.numel()}));
  return {index, c};
}

// Build a permutation index consisting of two parts:
// 1. the first part is the input parmeter 'p'
// 2. the second part is the index which is not in 'p' but in 'pi'
//
// e.g: p = {2, 4, 5}, pi = {0, 1, 2, 3, 4, 5},
//      output = {2, 4, 5, 0, 1, 3}
template <typename T>
spu::Value BuildPerm(spu::SPUContext* sctx, const spu::Value& p,
                     const spu::Value& pi) {
  auto tmp = hlo::Concatenate(sctx, {pi, p}, 0);
  auto soprf = hlo::SoPrf(sctx, tmp);
  // reveal after soprf
  auto soprf_revealed = hlo::Reveal(sctx, soprf);
  auto view = spu::NdArrayView<T>(soprf_revealed.data());
  std::set<T> p_set;
  for (int64_t i = pi.numel(); i < view.numel(); ++i) {
    auto [iter, ok] = p_set.insert(view[i]);
    YACL_ENFORCE(ok, "duplicate permutation");
  }
  spu::Index index;
  for (int64_t i = 0; i < pi.numel(); ++i) {
    if (p_set.find(view[i]) == p_set.end()) {
      index.push_back(i);
    }
  }
  YACL_ENFORCE(index.size() == pi.numel() - p.numel(),
               "size mismatch, index size: {}, expected: {}, collisions may "
               "happended in SoPrf",
               index.size(), pi.numel() - p.numel());

  return hlo::Concatenate(sctx, {p, hlo::LinearGather(sctx, tmp, index)}, 0);
};

}  // namespace

const std::string SecretJoin::kOpType("SecretJoin");

const std::string& SecretJoin::Type() const { return kOpType; }

void SecretJoin::Validate(ExecContext* ctx) {
  auto left_key = ctx->GetInput(kLeftKey);
  YACL_ENFORCE(left_key.size() > 0, "secret join cannot be empty");
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
  // TODO: support ABY3/CHEETAH after spu supported
  YACL_ENFORCE(ctx->GetSession()->GetSpuContext()->config().protocol ==
                   spu::ProtocolKind::SEMI2K,
               "secret join only support SEMI2K protocol now");
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
  SPDLOG_INFO("left row num: {}, right row num: {}", left_row_num,
              right_row_num);
  if (left_row_num == 0 || right_row_num == 0) {
    SetEmptyResult(ctx);
    return;
  }
  //
  // 1). Merge left and right table to count key group
  //
  spu::Value zeros = hlo::Constant(sctx, int64_t{0}, {left_row_num});
  spu::Value ones = hlo::Constant(sctx, int64_t{1}, {right_row_num});
  // [item_origin_mark] using '0' to mark the left rows and '1' to mark the
  // right.
  auto item_origin_mark = hlo::Concatenate(sctx, {zeros, ones}, 0);
  const auto [merged_key, merged_payload] = MergeInputs(ctx);

  std::vector<spu::Value> sort_inputs = merged_key;
  sort_inputs.push_back(item_origin_mark);
  sort_inputs.insert(sort_inputs.end(), merged_payload.begin(),
                     merged_payload.end());
  // sort_inputs: [keys, item_origin_mark, left_payloads, right_payloads]
  auto results = hlo::SimpleSort(
      sctx, sort_inputs, 0, hal::SortDirection::Ascending,
      merged_key.size() + 1 /* num_keys */, -1 /* valid_bits */);
  // accumulate count of left/right item in key group
  const auto [left_item_count, right_item_count, tmp_right_count] =
      CountKeyGroup(sctx,
                    std::vector<spu::Value>(
                        results.begin(), results.begin() + merged_key.size()),
                    results[merged_key.size()]);
  //
  // 2). Separation: sort by item_origin_mark, then separate left and right to
  // get table Ls and Rs
  //
  // sort inputs: [item_origin_mark, left_item_count, right_item_count,
  // tmp_right_count, left_payloads, right_payloads]
  std::vector<spu::Value> sep_inputs = {results[merged_key.size()],
                                        left_item_count, right_item_count,
                                        tmp_right_count};
  sep_inputs.insert(sep_inputs.end(), results.begin() + merged_key.size() + 1,
                    results.end());
  // NOTE: set valid_bits to 2 to contain the sign bit, though the
  // item_origin_mark only contains 0/1.
  // TODO: using index to replace payloads to reduce communication
  auto sep_sorted =
      hlo::SimpleSort(sctx, sep_inputs, 0, hal::SortDirection::Ascending,
                      1 /* num_keys */, 2 /* valid_bits */);
  // eb = (left_item_count * right_item_count) > 0, marks whether rows are in
  // join result, add 0 to generate ashare
  auto result_mark =
      hlo::Add(sctx,
               hlo::Greater(sctx, hlo::Mul(sctx, sep_sorted[1], sep_sorted[2]),
                            hlo::Constant(sctx, int64_t{0}, {total_row})),
               hlo::Constant(sctx, int64_t{0}, {total_row}));
  // Ls results, left table after separation
  // dL, payloads in Ls
  std::vector<spu::Value> ls_payload;
  for (int64_t i = 0; i < ctx->GetInput(kLeftPayload).size(); ++i) {
    ls_payload.push_back(
        hlo::Slice(sctx, sep_sorted[4 + i], {0}, {left_row_num}, {}));
  }
  // aR, right item count in Ls
  auto ls_right_count =
      hlo::Slice(sctx, sep_sorted[2], {0}, {left_row_num}, {});
  // eb, result mark in Ls
  auto ls_result_mark = hlo::Slice(sctx, result_mark, {0}, {left_row_num}, {});

  // Rs results, right table after separation
  // sR, tmp right count in Rs
  auto rs_tmp_right_count =
      hlo::Slice(sctx, sep_sorted[3], {left_row_num}, {total_row}, {});
  // aL, left item count in Rs
  auto rs_left_count =
      hlo::Slice(sctx, sep_sorted[1], {left_row_num}, {total_row}, {});
  // aR, right item count in Rs
  auto rs_right_count =
      hlo::Slice(sctx, sep_sorted[2], {left_row_num}, {total_row}, {});
  // eb, result mark in Rs
  auto rs_result_mark =
      hlo::Slice(sctx, result_mark, {left_row_num}, {total_row}, {});
  // dR, payloads in Rs
  std::vector<spu::Value> rs_payload;
  for (size_t i = 4 + ls_payload.size(); i < sep_sorted.size(); ++i) {
    rs_payload.push_back(
        hlo::Slice(sctx, sep_sorted[i], {left_row_num}, {total_row}, {}));
  }
  //
  // 3). Position Calculation
  //
  // TODO: using join upper bound to slice after permutation rather than in
  // result set
  auto [pl, size_left] = CountPosition(sctx, ls_right_count, ls_result_mark);
  auto [pr, size_right] = CountPosition(sctx, rs_left_count, rs_result_mark);

  // reveal upper bound for join result size
  auto upper_bound = hlo::Max(sctx, size_left, size_right);
  spu::NdArrayRef upper_bound_arr =
      spu::kernel::hal::dump_public(sctx, hlo::Reveal(sctx, upper_bound));
  int64_t m = upper_bound_arr.at<int64_t>({0});

  //
  // 4). Distribution
  //
  // generate permutation
  auto seq = hlo::Seal(sctx, hlo::Iota(sctx, spu::DataType::DT_I64, m));
  auto seq_shuffled = hlo::Shuffle(sctx, {seq}, 0)[0];

  spu::Value left_perm;
  spu::Value right_perm;
  if (sctx->config().field == spu::FM64) {
    left_perm = BuildPerm<uint64_t>(sctx, pl, seq_shuffled);
    right_perm = BuildPerm<uint64_t>(sctx, pr, seq_shuffled);
  } else if (sctx->config().field == spu::FM128) {
    left_perm = BuildPerm<uint128_t>(sctx, pl, seq_shuffled);
    right_perm = BuildPerm<uint128_t>(sctx, pr, seq_shuffled);
  } else {
    YACL_THROW("unsupported field type: {}", sctx->config().field);
  }

  // fill dummy to left: [ls_payload, dummy_mark]
  auto ld_inputs_filled = FillDummyValue(sctx, ls_payload, m - left_row_num);
  ld_inputs_filled.push_back(hlo::Seal(
      sctx,
      hlo::Concatenate(sctx,
                       {hlo::Constant(sctx, int64_t{0}, {left_row_num}),
                        hlo::Constant(sctx, int64_t{1}, {m - left_row_num})},
                       0)));
  // fill dummy to right: [rs_payload, rs_result_mark, rs_tmp_right_count,
  // rs_left_count, rs_right_count, dummy_mark]
  std::vector<spu::Value> rd_inputs = rs_payload;
  rd_inputs.push_back(rs_result_mark);
  rd_inputs.push_back(rs_tmp_right_count);
  rd_inputs.push_back(rs_left_count);
  rd_inputs.push_back(rs_right_count);
  auto rd_inputs_filled = FillDummyValue(sctx, rd_inputs, m - right_row_num);
  rd_inputs_filled.push_back(hlo::Seal(
      sctx,
      hlo::Concatenate(sctx,
                       {hlo::Constant(sctx, int64_t{0}, {right_row_num}),
                        hlo::Constant(sctx, int64_t{1}, {m - right_row_num})},
                       0)));

  // perm
  // Ld, the left table after permutation
  auto ld_permed = hlo::InvPermute(sctx, ld_inputs_filled, left_perm, 0);
  // Rd, the right table after permutation
  auto rd_permed = hlo::InvPermute(sctx, rd_inputs_filled, right_perm, 0);
  //
  // 5). Expansion
  //
  // Le, the left table after expansion
  auto le_results = util::ExpandGroupValueReversely(
      sctx, std::vector<spu::Value>(ld_permed.begin(), ld_permed.end() - 1),
      *ld_permed.rbegin());
  // Re, the right table after expansion
  auto re_results = util::ExpandGroupValueReversely(
      sctx, std::vector<spu::Value>(rd_permed.begin(), rd_permed.end() - 1),
      *rd_permed.rbegin());
  //
  // 6) Alignment: only right payload need to be aligned
  //
  // aDL, left item count in Rd
  auto re_left_count = rd_permed[rs_payload.size() + 2];
  // eb, result mark in Re
  auto re_results_mark = re_results[rs_payload.size()];

  auto a_eb = hlo::Sub(sctx, re_left_count, re_results_mark);

  // ai, the copy count before the item in the same group
  auto tmp_a = hlo::Seal(sctx, hlo::Constant(sctx, int64_t{0}, {1}));
  std::vector<spu::Value> a_slice;
  for (int64_t i = re_left_count.numel() - 1; i >= 0; i--) {
    tmp_a = hlo::Add(sctx, tmp_a, hlo::Slice(sctx, a_eb, {i}, {i + 1}, {}));
    a_slice.push_back(tmp_a);
  }
  auto copy_count = hlo::Reverse(sctx, hlo::Concatenate(sctx, a_slice, 0), {0});

  // (sR - 1) * (aL - 1)
  auto re_ones = hlo::Constant(sctx, int64_t{1}, {re_left_count.numel()});
  auto sr_1_al_1 =
      hlo::Mul(sctx, hlo::Sub(sctx, re_results[rs_payload.size() + 1], re_ones),
               hlo::Sub(sctx, re_results[rs_payload.size() + 2], re_ones));
  // aR - 1
  auto ar_1 = hlo::Sub(sctx, re_results[rs_payload.size() + 3], re_ones);

  // bi = i - (sR - 1) * (aL - 1) + ai * (aR - 1), the position for right
  // payload to replace
  auto dr_pos = hlo::Add(sctx, hlo::Sub(sctx, seq, sr_1_al_1),
                         hlo::Mul(sctx, copy_count, ar_1));

  // pi = re * (bi - i) + i, the permutation for Rf (final table R)
  auto rf_pi = hlo::Add(
      sctx, hlo::Mul(sctx, re_results_mark, hlo::Sub(sctx, dr_pos, seq)), seq);

  auto rf_results = hlo::InvPermute(
      sctx,
      std::vector<spu::Value>(re_results.begin(),
                              re_results.begin() + rs_payload.size()),
      rf_pi, 0);

  // TODO: reveal join size in hlo::Reduce
  auto join_size = hlo::Constant(sctx, int64_t{0}, {1});
  for (int64_t i = 0; i < re_results_mark.numel(); ++i) {
    join_size = hlo::Add(sctx, join_size,
                         hlo::Slice(sctx, re_results_mark, {i}, {i + 1}, {}));
  }
  auto revealed_join_size =
      hal::dump_public(sctx, hlo::Reveal(sctx, join_size)).at<int64_t>(0);
  SPDLOG_INFO("join result size:{}", revealed_join_size);

  // set result
  auto left_output = ctx->GetOutput(kOutLeft);
  for (int64_t i = 0; i < left_output.size(); ++i) {
    symbols->setVar(
        util::SpuVarNameEncoder::GetValueName(left_output[i].name()),
        hlo::Slice(sctx, le_results[i], {0}, {revealed_join_size}, {}));
  }
  auto right_output = ctx->GetOutput(kOutRight);
  for (int64_t i = 0; i < right_output.size(); ++i) {
    symbols->setVar(
        util::SpuVarNameEncoder::GetValueName(right_output[i].name()),
        hlo::Slice(sctx, rf_results[i], {0}, {revealed_join_size}, {}));
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
  auto* symbols = ctx->GetSession()->GetDeviceSymbols();
  auto* sctx = ctx->GetSession()->GetSpuContext();
  // concatenate key
  int64_t left_row_num = 0;
  int64_t right_row_num = 0;
  std::vector<spu::Value> merged_keys;
  for (int64_t i = 0; i < ctx->GetInput(kLeftKey).size(); ++i) {
    auto left_key = symbols->getVar(util::SpuVarNameEncoder::GetValueName(
        ctx->GetInput(kLeftKey)[i].name()));
    left_row_num = left_key.numel();
    auto right_key = symbols->getVar(util::SpuVarNameEncoder::GetValueName(
        ctx->GetInput(kRightKey)[i].name()));
    right_row_num = right_key.numel();

    merged_keys.push_back(hlo::Concatenate(sctx, {left_key, right_key}, 0));
  }

  // fill payloads with dummy items
  auto lz = hlo::Seal(sctx, hlo::Constant(sctx, int64_t{0}, {right_row_num}));
  auto rz = hlo::Seal(sctx, hlo::Constant(sctx, int64_t{0}, {left_row_num}));
  std::vector<spu::Value> payloads;
  for (const auto& input : ctx->GetInput(kLeftPayload)) {
    auto left_value =
        symbols->getVar(util::SpuVarNameEncoder::GetValueName(input.name()));
    // add dummy items to the end of left value
    auto tmp = hlo::Concatenate(
        sctx, {left_value, hal::dtype_cast(sctx, lz, left_value.dtype())}, 0);
    payloads.push_back(tmp);
  }
  for (const auto& input : ctx->GetInput(kRightPayload)) {
    auto right_value =
        symbols->getVar(util::SpuVarNameEncoder::GetValueName(input.name()));
    // add dummy items to the begin of right value
    auto tmp = hlo::Concatenate(
        sctx, {hal::dtype_cast(sctx, rz, right_value.dtype()), right_value}, 0);
    payloads.push_back(tmp);
  }

  return std::make_pair(merged_keys, payloads);
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
// left_count, right_count, tmp_right_count
//          1,           2,               0
//          1,           2,               1
//          1,           2,               2
//          0,           1,               1
std::tuple<spu::Value, spu::Value, spu::Value> SecretJoin::CountKeyGroup(
    spu::SPUContext* sctx, const std::vector<spu::Value>& keys,
    const spu::Value& item_origin_mark) {
  // 1. calculate key group mark, where '0' mark the begining of key group
  auto total_row = keys[0].numel();
  auto key_equal =
      hlo::Equal(sctx, hlo::Slice(sctx, keys[0], {0}, {total_row - 1}, {}),
                 hlo::Slice(sctx, keys[0], {1}, {total_row}, {}));
  for (size_t i = 1; i < keys.size(); ++i) {
    auto tmp_equal =
        hlo::Equal(sctx, hlo::Slice(sctx, keys[i], {0}, {total_row - 1}, {}),
                   hlo::Slice(sctx, keys[i], {1}, {total_row}, {}));
    key_equal = hlo::And(sctx, key_equal, tmp_equal);
  }
  auto zero = hlo::Seal(sctx, hlo::Constant(sctx, false, {1}));
  // TODO: cast to ashare first?
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
  auto left_count =
      util::ExpandGroupValueReversely(sctx, {tmp_left_count}, shifted_mark);
  auto right_count =
      util::ExpandGroupValueReversely(sctx, {tmp_right_count}, shifted_mark);

  return std::make_tuple(left_count[0], right_count[0], tmp_right_count);
}

void SecretJoin::SetEmptyResult(ExecContext* ctx) {
  auto* symbols = ctx->GetSession()->GetDeviceSymbols();
  auto* sctx = ctx->GetSession()->GetSpuContext();
  auto left_output = ctx->GetOutput(kOutLeft);
  for (int64_t i = 0; i < left_output.size(); ++i) {
    auto v = symbols->getVar(util::SpuVarNameEncoder::GetValueName(
        ctx->GetInput(kLeftPayload)[i].name()));
    symbols->setVar(
        util::SpuVarNameEncoder::GetValueName(left_output[i].name()),
        hlo::Slice(sctx, v, {0}, {0}, {}));
  }
  auto right_output = ctx->GetOutput(kOutRight);
  for (int64_t i = 0; i < right_output.size(); ++i) {
    auto v = symbols->getVar(util::SpuVarNameEncoder::GetValueName(
        ctx->GetInput(kRightPayload)[i].name()));
    symbols->setVar(
        util::SpuVarNameEncoder::GetValueName(right_output[i].name()),
        hlo::Slice(sctx, v, {0}, {0}, {}));
  }
}

}  // namespace scql::engine::op
