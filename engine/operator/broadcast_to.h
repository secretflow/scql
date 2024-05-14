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

class BroadcastTo : public Operator {
 public:
  static const std::string kOpType;

  static constexpr char kIn[] = "In";
  static constexpr char kShapeRefTensor[] = "ShapeRefTensor";
  static constexpr char kOut[] = "Out";

  const std::string& Type() const override;

 protected:
  void Validate(ExecContext* ctx) override;
  void Execute(ExecContext* ctx) override;

 private:
  void BroadcastToPrivate(ExecContext* ctx, const RepeatedPbTensor& input_pbs,
                          const RepeatedPbTensor& output_pbs,
                          const int64_t to_length);

  void BroadcastToPublic(ExecContext* ctx, const RepeatedPbTensor& input_pbs,
                         const RepeatedPbTensor& output_pbs,
                         const int64_t to_length);
};

}  // namespace scql::engine::op