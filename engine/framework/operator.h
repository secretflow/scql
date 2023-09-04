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

#include <string>

#include "engine/framework/exec.h"

namespace scql::engine {

class Operator {
 public:
  virtual ~Operator() = default;

  // operator type name.
  virtual const std::string& Type() const = 0;

  void Run(ExecContext* ctx) {
    Validate(ctx);
    Execute(ctx);
    ManageTensorLifecycle(ctx);
  }

 protected:
  // It will throw exception if validation fails
  virtual void Validate(ExecContext* ctx) {}
  virtual void Execute(ExecContext* ctx) = 0;
  void ManageTensorLifecycle(ExecContext* ctx);
};

}  // namespace scql::engine