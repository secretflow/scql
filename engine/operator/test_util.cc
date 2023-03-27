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

#include "engine/framework/session.h"
#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::op::test {

static yacl::link::FactoryMem g_mem_link_factory;

namespace {

pb::SessionStartParams::Party BuildParty(const std::string& code,
                                         int32_t rank) {
  pb::SessionStartParams::Party party;
  party.set_code(code);
  party.set_name("party " + code);
  party.set_host(code + ".com");
  party.set_rank(rank);
  return party;
}

pb::TensorList ToTensorList(const std::vector<pb::Tensor>& tensors) {
  pb::TensorList list;
  for (size_t i = 0; i < tensors.size(); ++i) {
    auto new_tensor = list.add_tensors();
    new_tensor->CopyFrom(tensors[i]);
  }
  return list;
}

spu::RuntimeConfig MakeSpuRuntimeConfigForTest(
    const spu::ProtocolKind protocol_kind) {
  spu::RuntimeConfig config;
  config.set_protocol(protocol_kind);
  config.set_field(spu::FieldType::FM64);
  config.set_sigmoid_mode(spu::RuntimeConfig::SIGMOID_REAL);
  return config;
}

}  // namespace

Session Make1PCSession(Router* ds_router, DatasourceAdaptorMgr* ds_mgr) {
  pb::SessionStartParams params;
  params.set_party_code(kPartyAlice);
  params.set_session_id("1PC-session");
  params.mutable_spu_runtime_cfg()->CopyFrom(
      MakeSpuRuntimeConfigForTest(spu::ProtocolKind::REF2K));
  SessionOptions options;
  auto alice = params.add_parties();
  alice->CopyFrom(BuildParty(kPartyAlice, 0));

  return Session(options, params, &g_mem_link_factory, nullptr, ds_router,
                 ds_mgr);
}

std::vector<Session> Make2PCSession(const spu::ProtocolKind protocol_kind) {
  auto alice = BuildParty(kPartyAlice, 0);
  auto bob = BuildParty(kPartyBob, 1);

  pb::SessionStartParams common_params;
  common_params.set_session_id("session_2pc");
  {
    auto p1 = common_params.add_parties();
    p1->CopyFrom(alice);
    auto p2 = common_params.add_parties();
    p2->CopyFrom(bob);
  }
  common_params.mutable_spu_runtime_cfg()->CopyFrom(
      MakeSpuRuntimeConfigForTest(protocol_kind));

  std::vector<std::future<Session>> futures(2);
  SessionOptions options;
  auto create_session = [&](const pb::SessionStartParams& params) {
    return Session(options, params, &g_mem_link_factory, nullptr, nullptr,
                   nullptr);
  };
  {
    pb::SessionStartParams params;
    params.CopyFrom(common_params);
    params.set_party_code(kPartyAlice);
    futures[0] = std::async(create_session, params);
  }
  {
    pb::SessionStartParams params;
    params.CopyFrom(common_params);
    params.set_party_code(kPartyBob);
    futures[1] = std::async(create_session, params);
  }

  std::vector<Session> results;
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
                               pb::TensorStatus visibility) {
  pb::Tensor tensor;
  tensor.set_name(name);
  tensor.set_elem_type(dtype);
  tensor.set_option(pb::TensorOptions::REFERENCE);
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
  auto tensor_table = ctx->GetTensorTable();
  for (const auto& named_tensor : ts) {
    tensor_table->AddTensor(named_tensor.name, named_tensor.tensor);
  }
}

namespace {
void FeedInputAsShares(const std::vector<ExecContext*>& ctxs,
                       const test::NamedTensor& input, spu::Visibility vtype) {
  auto proc = [](ExecContext* ctx, const test::NamedTensor& input,
                 spu::Visibility vtype) {
    spu::device::ColocatedIo cio(ctx->GetSession()->GetSpuHalContext());

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
    future.wait();
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
    util::SpuOutfeedHelper io(ctx->GetSession()->GetSpuHalContext(),
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

}  // namespace scql::engine::op::test
