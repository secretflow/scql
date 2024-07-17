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

#include "engine/operator/plain_join.h"

#include <unordered_map>
#include <vector>

#include "engine/core/primitive_builder.h"
#include "engine/core/tensor.h"
#include "engine/framework/exec.h"
#include "engine/util/communicate_helper.h"
#include "engine/util/psi_helper.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op {

using IndexMap = std::unordered_map<std::string, std::vector<uint32_t>>;

const std::string PlainJoin::kOpType("PlainJoin");

const std::string& PlainJoin::Type() const { return kOpType; }

void PlainJoin::Validate(ExecContext* ctx) {
  const auto& left = ctx->GetInput(kInLeft);
  const auto& right = ctx->GetInput(kInRight);
  const auto& left_out = ctx->GetOutput(kOutLeftJoinIndex);
  const auto& right_out = ctx->GetOutput(kOutRightJoinIndex);
  YACL_ENFORCE(
      left.size() >= 1 && right.size() == left.size(),
      "PlainJoin operator inputs Left and Right should be the same and "
      "larger than 1, but got size(Left)={}, size(Right)={}",
      left.size(), right.size());
  YACL_ENFORCE(util::AreTensorsStatusMatched(left, pb::TENSORSTATUS_PRIVATE),
               "PlainJoin operator input Left status should be private");
  YACL_ENFORCE(util::AreTensorsStatusMatched(right, pb::TENSORSTATUS_PRIVATE),
               "PlainJoin operator input Right status should be private");
  YACL_ENFORCE(
      util::AreTensorsStatusMatched(left_out, pb::TENSORSTATUS_PRIVATE),
      "PlainJoin operator output Left status should be private");
  YACL_ENFORCE(
      util::AreTensorsStatusMatched(right_out, pb::TENSORSTATUS_PRIVATE),
      "PlainJoin operator output Right status should be private");
}

std::vector<std::string> PlainJoin::Encode(
    const std::vector<TensorPtr>& tensors) {
  auto batch_provider =
      std::make_shared<util::BatchProvider>(tensors, FLAGS_provider_batch_size);
  std::vector<std::string> strs;
  while (true) {
    auto batch_strs = batch_provider->ReadNextBatch();
    if (batch_strs.empty()) {
      break;
    }
    strs.insert(strs.end(), batch_strs.begin(), batch_strs.end());
  }

  return strs;
}

void PlainJoin::Execute(ExecContext* ctx) {
  const auto& left = ctx->GetInput(kInLeft);
  const auto& right = ctx->GetInput(kInRight);

  // encode join keys
  auto tensor_table = ctx->GetSession()->GetTensorTable();
  std::vector<uint32_t> left_crcs(
      tensor_table->GetTensor(left[0].name())->Length(), 0);
  std::vector<uint32_t> right_crcs(
      tensor_table->GetTensor(right[0].name())->Length(), 0);

  // encode join keys
  std::vector<TensorPtr> left_ts;
  std::vector<TensorPtr> right_ts;
  for (int i = 0; i < left.size(); ++i) {
    const auto left_pb = left[i];
    auto left_t = tensor_table->GetTensor(left_pb.name());
    YACL_ENFORCE(left_t != nullptr, "input {} not found in tensor table",
                 left_pb.name());
    left_ts.push_back(left_t);

    const auto right_pb = right[i];
    auto right_t = tensor_table->GetTensor(right_pb.name());
    YACL_ENFORCE(right_t != nullptr, "input {} not found in tensor table",
                 right_pb.name());
    right_ts.push_back(right_t);
  }
  auto left_strs = Encode(left_ts);
  auto right_strs = Encode(right_ts);

  // build RightIndexMap
  IndexMap right_index_map;
  for (uint32_t index = 0; index < right_strs.size(); index++) {
    if (right_index_map.find(right_strs[index]) == right_index_map.end()) {
      right_index_map[right_strs[index]] = {};
    }
    right_index_map[right_strs[index]].push_back(index);
  }

  // compute out index
  Int64TensorBuilder left_builder;
  Int64TensorBuilder right_builder;
  for (size_t left_index = 0; left_index < left_strs.size(); left_index++) {
    const auto& left_str = left_strs[left_index];
    if (right_index_map.find(left_str) != right_index_map.end()) {
      const auto& right_indexs = right_index_map[left_str];
      for (const auto& right_index : right_indexs) {
        left_builder.Append(left_index);
        right_builder.Append(right_index);
      }
    }
  }

  TensorPtr left_indices;
  left_builder.Finish(&left_indices);
  const auto& left_out = ctx->GetOutput(kOutLeftJoinIndex);
  tensor_table->AddTensor(left_out[0].name(), std::move(left_indices));

  TensorPtr right_indices;
  right_builder.Finish(&right_indices);
  const auto& right_out = ctx->GetOutput(kOutRightJoinIndex);
  tensor_table->AddTensor(right_out[0].name(), std::move(right_indices));
}

}  // namespace scql::engine::op