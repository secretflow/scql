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

#include <cmath>
#include <functional>
#include <vector>

#include "gtest/gtest.h"

#include "engine/datasource/datasource_adaptor_mgr.h"
#include "engine/datasource/router.h"
#include "engine/framework/exec.h"
#include "engine/framework/operator.h"
#include "engine/framework/session.h"

#ifndef TestParamNameGenerator
#define TestParamNameGenerator(TestCaseClass)                                \
  [](const testing::TestParamInfo<TestCaseClass::ParamType>& info) {         \
    return fmt::format(                                                      \
        "{}{}p{}{}", info.index,                                             \
        spu::GetProtocolKindName(std::get<0>(info.param).protocol),          \
        std::get<0>(info.param).party_size,                                  \
        std::get<0>(info.param).enable_colocated_optimization ? "opt" : ""); \
  }
#endif

namespace scql::engine::op::test {

// predifined party codes for tests
constexpr char kPartyAlice[] = "alice";
constexpr char kPartyBob[] = "bob";
constexpr char kPartyCarol[] = "carol";
constexpr const char* kPartyCodes[] = {"alice", "bob", "carol"};

spu::RuntimeConfig GetSpuRuntimeConfigForTest();

struct SpuRuntimeTestCase {
  spu::ProtocolKind protocol;
  size_t party_size;
  bool enable_colocated_optimization;
};

static const auto SpuTestValues2PC = testing::Values(
    test::SpuRuntimeTestCase{spu::ProtocolKind::CHEETAH, 2, true},
    test::SpuRuntimeTestCase{spu::ProtocolKind::SEMI2K, 2, true},
    test::SpuRuntimeTestCase{spu::ProtocolKind::CHEETAH, 2, false},
    test::SpuRuntimeTestCase{spu::ProtocolKind::SEMI2K, 2, false});

static const auto SpuTestValuesMultiPC = testing::Values(
    test::SpuRuntimeTestCase{spu::ProtocolKind::CHEETAH, 2, true},
    test::SpuRuntimeTestCase{spu::ProtocolKind::SEMI2K, 2, true},
    test::SpuRuntimeTestCase{spu::ProtocolKind::SEMI2K, 3, true},
    test::SpuRuntimeTestCase{spu::ProtocolKind::ABY3, 3, true},
    test::SpuRuntimeTestCase{spu::ProtocolKind::CHEETAH, 2, false},
    test::SpuRuntimeTestCase{spu::ProtocolKind::SEMI2K, 2, false},
    test::SpuRuntimeTestCase{spu::ProtocolKind::SEMI2K, 3, false},
    test::SpuRuntimeTestCase{spu::ProtocolKind::ABY3, 3, false});

static const auto SpuTestValuesMultiPCDisableColocated = testing::Values(
    test::SpuRuntimeTestCase{spu::ProtocolKind::CHEETAH, 2, false},
    test::SpuRuntimeTestCase{spu::ProtocolKind::SEMI2K, 2, false},
    test::SpuRuntimeTestCase{spu::ProtocolKind::SEMI2K, 3, false},
    test::SpuRuntimeTestCase{spu::ProtocolKind::ABY3, 3, false});

pb::JobStartParams::Party BuildParty(const std::string& code, int32_t rank);

spu::RuntimeConfig MakeSpuRuntimeConfigForTest(
    spu::ProtocolKind protocol_kind,
    bool enable_colocated_optimization = false);

// make single party session
std::shared_ptr<Session> Make1PCSession(Router* ds_router = nullptr,
                                        DatasourceAdaptorMgr* ds_mgr = nullptr);

// Make Multi PC session
std::vector<std::shared_ptr<Session> > MakeMultiPCSession(
    SpuRuntimeTestCase test_case);

class ExecNodeBuilder {
 public:
  explicit ExecNodeBuilder(const std::string& op_type);

  pb::ExecNode Build();

  ExecNodeBuilder& SetNodeName(const std::string& node_name);

  ExecNodeBuilder& AddInput(const std::string& name, pb::TensorList tensors);
  ExecNodeBuilder& AddInput(const std::string& name,
                            const std::vector<pb::Tensor>& tensors);

  ExecNodeBuilder& AddOutput(const std::string& name, pb::TensorList tensors);
  ExecNodeBuilder& AddOutput(const std::string& name,
                             const std::vector<pb::Tensor>& tensors);

  ExecNodeBuilder& AddStringAttr(const std::string& name,
                                 const std::string& value);
  ExecNodeBuilder& AddStringsAttr(const std::string& name,
                                  const std::vector<std::string>& values);

  ExecNodeBuilder& AddInt64Attr(const std::string& name, int64_t value);
  ExecNodeBuilder& AddInt64sAttr(const std::string& name,
                                 const std::vector<int64_t>& values);
  ExecNodeBuilder& AddDoubleAttr(const std::string& name, double value);
  ExecNodeBuilder& AddDoubleAttrs(const std::string& name,
                                  const std::vector<double>& value);

  ExecNodeBuilder& AddBooleanAttr(const std::string& name, bool value);

  ExecNodeBuilder& AddAttr(const std::string& name, const pb::Tensor& tensor);

 private:
  pb::ExecNode node_;
};

pb::Tensor MakeTensorReference(const std::string& name,
                               pb::PrimitiveDataType dtype,
                               pb::TensorStatus visibility, int ref_count = 1);

inline pb::Tensor MakePrivateTensorReference(const std::string& name,
                                             pb::PrimitiveDataType dtype) {
  return MakeTensorReference(name, dtype,
                             pb::TensorStatus::TENSORSTATUS_PRIVATE);
}

inline pb::Tensor MakeSecretTensorReference(const std::string& name,
                                            pb::PrimitiveDataType dtype) {
  return MakeTensorReference(name, dtype,
                             pb::TensorStatus::TENSORSTATUS_SECRET);
}

pb::Tensor MakeTensorAs(const std::string& name, const pb::Tensor& ref);

struct NamedTensor {
  std::string name;
  TensorPtr tensor;

  NamedTensor(std::string name, TensorPtr tensor)
      : name(std::move(name)), tensor(std::move(tensor)) {}
};

void FeedInputsAsPrivate(ExecContext* ctx,
                         const std::vector<test::NamedTensor>& ts);

void FeedInputsAsPublic(const std::vector<ExecContext*>& ctxs,
                        const std::vector<test::NamedTensor>& ts);

void FeedInputsAsSecret(const std::vector<ExecContext*>& ctxs,
                        const std::vector<test::NamedTensor>& ts);

// assume ctxs[0] is ExecContext for rank 0
TensorPtr RevealSecret(const std::vector<ExecContext*>& ctxs,
                       const std::string& name);

using OpCreator = std::function<std::unique_ptr<Operator>()>;

void RunOpAsync(const std::vector<ExecContext*>& exec_ctxs,
                OpCreator create_op_fn);

template <class Op>
void RunAsync(const std::vector<ExecContext*>& exec_ctxs) {
  RunOpAsync(exec_ctxs, []() { return std::make_unique<Op>(); });
}

}  // namespace scql::engine::op::test
