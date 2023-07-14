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

class ObliviousGroupAggBase : public Operator {
 public:
  static constexpr char kGroup[] = "Group";
  static constexpr char kIn[] = "In";
  static constexpr char kOut[] = "Out";

 public:
  virtual spu::Value HandleEmptyInput(const spu::Value& in) { return in; }

  virtual spu::Value CalculateResult(spu::SPUContext* sctx,
                                     const spu::Value& value,
                                     const spu::Value& group_value) = 0;

 protected:
  void Validate(ExecContext* ctx) override;

  void Execute(ExecContext* ctx) override;
};

class ObliviousGroupSum : public ObliviousGroupAggBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 public:
  spu::Value HandleEmptyInput(const spu::Value& in) override {
    // Sum(bool tensor) should return int64
    if (in.dtype() == spu::DT_I1) {
      return in.clone().setDtype(spu::DT_I64, true);
    } else {
      return in;
    }
  }

  spu::Value CalculateResult(spu::SPUContext* sctx, const spu::Value& value,
                             const spu::Value& group_value) override;
};

class ObliviousGroupCount : public ObliviousGroupAggBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 public:
  spu::Value HandleEmptyInput(const spu::Value& in) override {
    return in.clone().setDtype(spu::DT_I64, true);
  }

  spu::Value CalculateResult(spu::SPUContext* sctx, const spu::Value& value,
                             const spu::Value& group_value) override;
};

class ObliviousGroupAvg : public ObliviousGroupAggBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 public:
  spu::Value HandleEmptyInput(const spu::Value& in) override {
    return in.clone().setDtype(spu::DT_F64, true);
  }

  spu::Value CalculateResult(spu::SPUContext* sctx, const spu::Value& value,
                             const spu::Value& group_value) override;
};

class ObliviousGroupMax : public ObliviousGroupAggBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 public:
  spu::Value CalculateResult(spu::SPUContext* sctx, const spu::Value& value,
                             const spu::Value& group_value) override;
};

class ObliviousGroupMin : public ObliviousGroupAggBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 public:
  spu::Value CalculateResult(spu::SPUContext* sctx, const spu::Value& value,
                             const spu::Value& group_value) override;
};
// TODO(jingshi) : Add ObliviousGroupMedian.

}  // namespace scql::engine::op
