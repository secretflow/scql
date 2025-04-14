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

#include "arrow/compute/api.h"
#include "arrow/compute/exec.h"

#include "engine/framework/operator.h"

namespace scql::engine::op {

class ReduceBase : public Operator {
 public:
  static constexpr char kIn[] = "In";
  static constexpr char kOut[] = "Out";

 public:
  void Validate(ExecContext* ctx) override;
  void Execute(ExecContext* ctx) override;

 protected:
  virtual std::string GetArrowFunName() = 0;

  virtual spu::Value SecretReduceImpl(spu::SPUContext* sctx,
                                      const spu::Value& in);
  virtual void ExecuteInPlain(ExecContext* ctx, const pb::Tensor& in,
                              const pb::Tensor& out);

  virtual spu::Value HandleEmptyInput(const spu::Value& in) { return in; }

  virtual void AggregateInit(spu::SPUContext* sctx, const spu::Value& in) {}
  /// @returns reduce init value
  virtual spu::Value GetInitValue(spu::SPUContext* sctx) = 0;

  using ReduceFn =
      std::function<spu::Value(const spu::Value& lhs, const spu::Value& rhs)>;

  virtual ReduceFn GetReduceFn(spu::SPUContext* sctx) = 0;

  virtual spu::Value AggregateFinalize(spu::SPUContext* sctx,
                                       const spu::Value& value) {
    return value;
  }
  virtual void InitAttribute(ExecContext* ctx) {}
};

class ReduceSum : public ReduceBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 protected:
  std::string GetArrowFunName() override { return "sum"; }

  spu::Value GetInitValue(spu::SPUContext* sctx) override;
  ReduceFn GetReduceFn(spu::SPUContext* sctx) override;
};

class ReduceCount : public ReduceBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 protected:
  std::string GetArrowFunName() override { return "count"; }

  spu::Value GetInitValue(spu::SPUContext* sctx) override;
  ReduceFn GetReduceFn(spu::SPUContext* sctx) override;
};

class ReduceAvg : public ReduceBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 protected:
  std::string GetArrowFunName() override { return "mean"; }

  spu::Value HandleEmptyInput(const spu::Value& in) override;

  void AggregateInit(spu::SPUContext* sctx, const spu::Value& in) override;

  spu::Value GetInitValue(spu::SPUContext* sctx) override;
  ReduceFn GetReduceFn(spu::SPUContext* sctx) override;

  spu::Value AggregateFinalize(spu::SPUContext* sctx,
                               const spu::Value& sum) override;

 private:
  int64_t count_ = 0;
};

class ReduceMin : public ReduceBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 protected:
  std::string GetArrowFunName() override { return "min"; }

  void AggregateInit(spu::SPUContext* sctx, const spu::Value& in) override;

  spu::Value GetInitValue(spu::SPUContext* sctx) override;
  ReduceFn GetReduceFn(spu::SPUContext* sctx) override;

 private:
  spu::Value init_value_;
};

class ReduceMax : public ReduceBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override;

 protected:
  std::string GetArrowFunName() override { return "max"; }

  void AggregateInit(spu::SPUContext* sctx, const spu::Value& in) override;

  spu::Value GetInitValue(spu::SPUContext* sctx) override;
  ReduceFn GetReduceFn(spu::SPUContext* sctx) override;

 private:
  spu::Value init_value_;
};

class ReducePercentileDisc : public ReduceBase {
 public:
  static const std::string kOpType;
  static constexpr char kPercent[] = "percent";
  const std::string& Type() const override;

 protected:
  std::string GetArrowFunName() override {
    YACL_THROW("should not reach here");
  }
  void AggregateInit(spu::SPUContext* sctx, const spu::Value& in) override;
  spu::Value GetInitValue(spu::SPUContext* sctx) override;
  ReduceFn GetReduceFn(spu::SPUContext* sctx) override;
  void InitAttribute(ExecContext* ctx) override;
  spu::Value SecretReduceImpl(spu::SPUContext* sctx,
                              const spu::Value& in) override;
  void ExecuteInPlain(ExecContext* ctx, const pb::Tensor& in,
                      const pb::Tensor& out) override;

 private:
  double percent_;

  int64_t GetPointIndex(size_t length) const {
    int64_t pos = static_cast<size_t>(
        std::ceil(percent_ * static_cast<double>(length)) - 1);
    pos = std::max(pos, static_cast<int64_t>(0));
    pos = std::min(pos, static_cast<int64_t>(length) - 1);
    return pos;
  }
};

}  // namespace scql::engine::op