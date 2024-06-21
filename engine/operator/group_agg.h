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

class GroupAggBase : public Operator {
 public:
  static constexpr char kGroupId[] = "GroupId";
  static constexpr char kGroupNum[] = "GroupNum";
  static constexpr char kIn[] = "In";
  static constexpr char kOut[] = "Out";

 public:
  virtual std::shared_ptr<arrow::Scalar> AggImpl(
      std::shared_ptr<arrow::Array> arr) = 0;

 protected:
  void Validate(ExecContext* ctx) override;

  void Execute(ExecContext* ctx) override;

 private:
  uint32_t GetGroupNum(ExecContext* ctx);

  std::shared_ptr<arrow::Array> GetGroupId(ExecContext* ctx);

  // default use type of 'scalars' to build Tensor, if 'scalars' is empty, use
  // 'empty_type'
  TensorPtr BuildTensorFromScalarVector(
      const arrow::ScalarVector& scalars,
      std::shared_ptr<arrow::DataType> empty_type);
};

class GroupFirstOf : public GroupAggBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override { return kOpType; }

 public:
  std::shared_ptr<arrow::Scalar> AggImpl(
      std::shared_ptr<arrow::Array> arr) override;
};

class GroupCountDistinct : public GroupAggBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override { return kOpType; }

  std::shared_ptr<arrow::Scalar> AggImpl(
      std::shared_ptr<arrow::Array> arr) override;
};

class GroupCount : public GroupAggBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override { return kOpType; }

  std::shared_ptr<arrow::Scalar> AggImpl(
      std::shared_ptr<arrow::Array> arr) override;
};

class GroupSum : public GroupAggBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override { return kOpType; }

  std::shared_ptr<arrow::Scalar> AggImpl(
      std::shared_ptr<arrow::Array> arr) override;
};

class GroupAvg : public GroupAggBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override { return kOpType; }

  std::shared_ptr<arrow::Scalar> AggImpl(
      std::shared_ptr<arrow::Array> arr) override;
};

class GroupMin : public GroupAggBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override { return kOpType; }

  std::shared_ptr<arrow::Scalar> AggImpl(
      std::shared_ptr<arrow::Array> arr) override;
};

class GroupMax : public GroupAggBase {
 public:
  static const std::string kOpType;

  const std::string& Type() const override { return kOpType; }

  std::shared_ptr<arrow::Scalar> AggImpl(
      std::shared_ptr<arrow::Array> arr) override;
};

}  // namespace scql::engine::op
