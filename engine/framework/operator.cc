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

#include "engine/framework/operator.h"

#include <tuple>

#include "gflags/gflags.h"

DEFINE_bool(enable_tensor_life_cycle_manage, true,
            "whether tensor life cycle manage is enable/disable");

namespace scql::engine {

void Operator::ManageTensorLifecycle(ExecContext *ctx) {
  if (!FLAGS_enable_tensor_life_cycle_manage) {
    return;
  }
  auto in_tensors = ctx->GetInputTensors();
  auto out_tensors = ctx->GetOutputTensors();
  std::vector<std::string> in_tensor_names;
  for (const auto &tensor : in_tensors) {
    in_tensor_names.emplace_back(tensor.name());
  }
  Session::RefNums output_ref_nums;
  for (const auto &tensor : out_tensors) {
    output_ref_nums.emplace_back(tensor.name(), tensor.ref_num());
  }
  ctx->GetSession()->UpdateRefName(in_tensor_names, output_ref_nums);
};

}  // namespace scql::engine