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

/// @brief Shape return the shape of inputs
class Shape : public Operator {
 public:
  static const std::string kOpType;

  static constexpr char kIn[] = "In";
  static constexpr char kOut[] = "Out";
  static constexpr char kAxis[] = "axis";

  static constexpr int64_t kAxisDefault = -1;
  static constexpr int64_t kAxisRow = 0;
  static constexpr int64_t kAxisColumn = 1;

  const std::string& Type() const override;

 protected:
  void Validate(ExecContext* ctx) override;
  void Execute(ExecContext* ctx) override;

 private:
  TensorPtr CreateShapeTensor(ExecContext* ctx,
                              const std::pair<int64_t, int64_t>& shapes);
};

}  // namespace scql::engine::op