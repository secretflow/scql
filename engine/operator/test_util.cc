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

#include "engine/operator/test_util.h"

#include <memory>

#include "libspu/core/config.h"

#include "engine/framework/session.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op::test {

static yacl::link::FactoryMem g_mem_link_factory;

namespace {

pb::TensorList ToTensorList(const std::vector<pb::Tensor>& tensors) {
  pb::TensorList list;
  for (const auto& tensor : tensors) {
    auto* new_tensor = list.add_tensors();
    new_tensor->CopyFrom(tensor);
  }
  return list;
}

}  // namespace

pb::JobStartParams::Party BuildParty(const std::string& code, int32_t rank) {
  pb::JobStartParams::Party party;
  party.set_code(code);
  party.set_name("party " + code);
  party.set_host(code + ".com");
  party.set_rank(rank);
  return party;
}

spu::RuntimeConfig MakeSpuRuntimeConfigForTest(
    const spu::ProtocolKind protocol_kind) {
  spu::RuntimeConfig config;
  config.set_protocol(protocol_kind);
  config.set_field(spu::FieldType::FM64);
  config.set_sigmoid_mode(spu::RuntimeConfig::SIGMOID_REAL);
  config.set_experimental_enable_colocated_optimization(true);

  spu::populateRuntimeConfig(config);
  return config;
}

std::shared_ptr<Session> Make1PCSession(Router* ds_router,
                                        DatasourceAdaptorMgr* ds_mgr) {
  pb::JobStartParams params;
  params.set_party_code(kPartyAlice);
  params.set_job_id("1PC-session");
  params.set_time_zone("+08:00");
  params.mutable_spu_runtime_cfg()->CopyFrom(
      MakeSpuRuntimeConfigForTest(spu::ProtocolKind::REF2K));
  SessionOptions options;
  auto* alice = params.add_parties();
  alice->CopyFrom(BuildParty(kPartyAlice, 0));
  pb::DebugOptions debug_opts;
  // When there is only one party involved, the protocol will not be validated,
  // so the related parameters are dummy.
  std::vector<spu::ProtocolKind> allowed_protocols{spu::ProtocolKind::REF2K,
                                                   spu::ProtocolKind::CHEETAH,
                                                   spu::ProtocolKind::SEMI2K};
  return std::make_shared<Session>(options, params, debug_opts,
                                   &g_mem_link_factory, ds_router, ds_mgr,
                                   allowed_protocols);
}

std::vector<std::shared_ptr<Session>> MakeMultiPCSession(
    const SpuRuntimeTestCase test_case) {
  pb::JobStartParams common_params;
  common_params.set_job_id("session_multi_pc");
  for (size_t i = 0; i < test_case.party_size; ++i) {
    auto party = BuildParty(kPartyCodes[i], i);
    auto* p = common_params.add_parties();
    p->CopyFrom(party);
  }
  common_params.mutable_spu_runtime_cfg()->CopyFrom(
      MakeSpuRuntimeConfigForTest(test_case.protocol));

  std::vector<std::future<std::shared_ptr<Session>>> futures;
  SessionOptions options;
  options.psi_config.unbalance_psi_ratio_threshold = 5;
  options.psi_config.unbalance_psi_larger_party_rows_count_threshold = 81920;
  options.psi_config.psi_curve_type = psi::CURVE_FOURQ;
  auto create_session = [&](const pb::JobStartParams& params) {
    pb::DebugOptions debug_opts;
    std::vector<spu::ProtocolKind> allowed_protocols{spu::ProtocolKind::CHEETAH,
                                                     spu::ProtocolKind::SEMI2K,
                                                     spu::ProtocolKind::ABY3};
    return std::make_shared<Session>(options, params, debug_opts,
                                     &g_mem_link_factory, nullptr, nullptr,
                                     allowed_protocols);
  };
  for (size_t i = 0; i < test_case.party_size; ++i) {
    pb::JobStartParams params;
    params.CopyFrom(common_params);
    params.set_party_code(kPartyCodes[i]);
    futures.push_back(std::async(create_session, params));
  }

  std::vector<std::shared_ptr<Session>> results;
  for (size_t i = 0; i < futures.size(); i++) {
    results.push_back(futures[i].get());
  }

  return results;
}

ExecNodeBuilder::ExecNodeBuilder(const std::string& op_type) {
  node_.set_op_type(op_type);
}

ExecNodeBuilder& ExecNodeBuilder::SetNodeName(const std::string& node_name) {
  node_.set_node_name(node_name);
  return *this;
}

ExecNodeBuilder& ExecNodeBuilder::AddInput(const std::string& name,
                                           pb::TensorList tensors) {
  auto& inputs = *node_.mutable_inputs();
  inputs[name] = std::move(tensors);
  return *this;
}

ExecNodeBuilder& ExecNodeBuilder::AddInput(
    const std::string& name, const std::vector<pb::Tensor>& tensors) {
  return AddInput(name, ToTensorList(tensors));
}

ExecNodeBuilder& ExecNodeBuilder::AddOutput(const std::string& name,
                                            pb::TensorList tensors) {
  auto& outputs = *node_.mutable_outputs();
  outputs[name] = std::move(tensors);
  return *this;
}

ExecNodeBuilder& ExecNodeBuilder::AddOutput(
    const std::string& name, const std::vector<pb::Tensor>& tensors) {
  return AddOutput(name, ToTensorList(tensors));
}

ExecNodeBuilder& ExecNodeBuilder::AddStringAttr(const std::string& name,
                                                const std::string& value) {
  return AddStringsAttr(name, std::vector<std::string>{value});
}

ExecNodeBuilder& ExecNodeBuilder::AddStringsAttr(
    const std::string& name, const std::vector<std::string>& values) {
  auto& attrs = *node_.mutable_attributes();
  util::SetStringValues(attrs[name].mutable_t(), values);
  return *this;
}

ExecNodeBuilder& ExecNodeBuilder::AddInt64Attr(const std::string& name,
                                               int64_t value) {
  return AddInt64sAttr(name, std::vector<int64_t>{value});
}

ExecNodeBuilder& ExecNodeBuilder::AddInt64sAttr(const std::string& name,
                                                std::vector<int64_t> values) {
  auto& attrs = *node_.mutable_attributes();
  util::SetInt64Values(attrs[name].mutable_t(), values);
  return *this;
}

ExecNodeBuilder& ExecNodeBuilder::AddBooleanAttr(const std::string& name,
                                                 bool value) {
  auto& attrs = *node_.mutable_attributes();
  util::SetBooleanValues(attrs[name].mutable_t(), std::vector<bool>{value});
  return *this;
}

ExecNodeBuilder& ExecNodeBuilder::AddAttr(const std::string& name,
                                          const pb::Tensor& tensor) {
  auto& attrs = *node_.mutable_attributes();
  attrs[name].mutable_t()->CopyFrom(tensor);
  return *this;
}

pb::ExecNode ExecNodeBuilder::Build() { return node_; }

pb::Tensor MakeTensorReference(const std::string& name,
                               pb::PrimitiveDataType dtype,
                               pb::TensorStatus visibility, int ref_count) {
  pb::Tensor tensor;
  tensor.set_name(name);
  tensor.set_elem_type(dtype);
  tensor.set_option(pb::TensorOptions::REFERENCE);
  tensor.set_ref_num(ref_count);
  {
    auto& annotation = *tensor.mutable_annotation();
    annotation.set_status(visibility);
  }
  return tensor;
}

pb::Tensor MakeTensorAs(const std::string& name, const pb::Tensor& ref) {
  pb::Tensor tensor;
  tensor.CopyFrom(ref);
  tensor.set_name(name);
  return tensor;
}

void FeedInputsAsPrivate(ExecContext* ctx,
                         const std::vector<test::NamedTensor>& ts) {
  auto* tensor_table = ctx->GetTensorTable();
  for (const auto& named_tensor : ts) {
    tensor_table->AddTensor(named_tensor.name, named_tensor.tensor);
  }
}

namespace {
void FeedInputAsShares(const std::vector<ExecContext*>& ctxs,
                       const test::NamedTensor& input, spu::Visibility vtype) {
  auto proc = [](ExecContext* ctx, const test::NamedTensor& input,
                 spu::Visibility vtype) {
    spu::device::ColocatedIo cio(ctx->GetSession()->GetSpuContext());

    util::SpuInfeedHelper infeed_helper(&cio);
    if (ctx->GetSession()->GetLink()->Rank() == 0) {
      TensorPtr in_t = input.tensor;
      if (in_t->Type() == pb::PrimitiveDataType::STRING) {
        in_t = ctx->GetSession()->StringToHash(*in_t);
      }
      if (vtype == spu::VIS_SECRET) {
        infeed_helper.InfeedTensorAsSecret(input.name, *in_t);
      } else {
        infeed_helper.InfeedTensorAsPublic(input.name, *in_t);
      }
    }

    infeed_helper.Sync();

    auto& symbols = cio.deviceSymbols();
    ctx->GetSession()->MergeDeviceSymbolsFrom(symbols);
  };

  std::vector<std::future<void>> futures;
  for (size_t i = 0; i < ctxs.size(); ++i) {
    auto future = std::async(proc, ctxs[i], input, vtype);
    futures.push_back(std::move(future));
  }

  for (auto& future : futures) {
    future.get();
  }
}
}  // namespace

void FeedInputsAsSecret(const std::vector<ExecContext*>& ctxs,
                        const std::vector<test::NamedTensor>& ts) {
  for (size_t i = 0; i < ts.size(); ++i) {
    FeedInputAsShares(ctxs, ts[i], spu::VIS_SECRET);
  }
}

void FeedInputsAsPublic(const std::vector<ExecContext*>& ctxs,
                        const std::vector<test::NamedTensor>& ts) {
  for (size_t i = 0; i < ts.size(); ++i) {
    FeedInputAsShares(ctxs, ts[i], spu::VIS_PUBLIC);
  }
}

TensorPtr RevealSecret(const std::vector<ExecContext*>& ctxs,
                       const std::string& name) {
  auto reveal = [](ExecContext* ctx, const std::string& name) -> TensorPtr {
    util::SpuOutfeedHelper io(ctx->GetSession()->GetSpuContext(),
                              ctx->GetSession()->GetDeviceSymbols());
    // reveal to rank 0: alice
    return io.RevealTo(name, 0);
  };
  std::vector<std::future<TensorPtr>> futures;
  for (size_t i = 0; i < ctxs.size(); ++i) {
    auto future = std::async(reveal, ctxs[i], name);
    futures.push_back(std::move(future));
  }

  for (size_t i = 0; i < futures.size(); ++i) {
    futures[i].wait();
  }
  return futures[0].get();
}

void RunOpAsync(const std::vector<ExecContext*>& exec_ctxs,
                OpCreator create_op_fn) {
  std::vector<std::future<void>> futures(exec_ctxs.size());
  for (size_t idx = 0; idx < exec_ctxs.size(); ++idx) {
    futures[idx] = std::async(
        std::launch::async,
        [&](ExecContext* ectx) {
          auto op = create_op_fn();
          op->Run(ectx);
        },
        exec_ctxs[idx]);
  }

  bool is_throw = false;
  for (size_t idx = 0; idx < futures.size(); ++idx) {
    try {
      futures[idx].get();
    } catch (const std::exception& e) {
      SPDLOG_ERROR("catch throw for idx={}, exception={}", idx, e.what());
      is_throw = true;
    }
  }
  if (is_throw) {
    YACL_THROW("op async err");
  }
}

}  // namespace scql::engine::op::test
