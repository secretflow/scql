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

#pragma once

#include "engine/framework/operator.h"

namespace scql::engine::op {

/// @brief Filter using a filter tensor to choose the specific rows of data
/// tensors.(tensors including filter tensor have the same number of rows,
/// unlike the FilterByIndex Operator)
/// e.g: when Filter tensor = {1, 0, 1, 0, 0}, In tensor = {0, 1, 2, 3, 4},
/// after filtering the Out tensor = {0, 2}
class Filter : public Operator {
 public:
  static const std::string kOpType;

  static constexpr char kInFilter[] = "Filter";
  static constexpr char kInData[] = "In";
  static constexpr char kOut[] = "Out";

  const std::string& Type() const override;

 protected:
  void Validate(ExecContext* ctx) override;
  void Execute(ExecContext* ctx) override;

 private:
  static void FilterPrivate(ExecContext* ctx, const pb::Tensor& filter_pb,
                            const pb::Tensor& data_pb,
                            const std::string& output_name);

  static void FilterSecret(ExecContext* ctx, const pb::Tensor& filter_pb,
                           const pb::Tensor& data_pb,
                           const std::string& output_name);

  static TensorPtr GetPrivateOrPublicTensor(ExecContext* ctx,
                                            const pb::Tensor& input_pb);
};

}  // namespace scql::engine::op