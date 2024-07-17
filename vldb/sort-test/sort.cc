// Copyright 2024 Ant Group Co., Ltd.
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

// clang-format off
// > bazel run //vldb/sort-test:sort -c opt  -- --numel=1000 --parties=127.0.0.1:61530,127.0.0.1:61531
// clang-format on

#include <future>

#include "absl/strings/str_split.h"
#include "examples/cpp/utils.h"
#include "libspu/core/config.h"
#include "libspu/device/io.h"
#include "libspu/kernel/hal/permute.h"
#include "libspu/kernel/hal/prot_wrapper.h"
#include "libspu/kernel/hlo/const.h"
#include "libspu/kernel/hlo/shuffle.h"
#include "libspu/mpc/factory.h"
#include "llvm/Support/CommandLine.h"
#include "spdlog/spdlog.h"
#include "yacl/link/factory.h"
#include "yacl/utils/elapsed_timer.h"

#include "libspu/spu.pb.h"

using namespace spu::kernel;

llvm::cl::opt<std::string> Parties(
    "parties", llvm::cl::init("127.0.0.1:61530,127.0.0.1:61531"),
    llvm::cl::desc("server list, format: host1:port1[,host2:port2, ...]"));
llvm::cl::opt<int64_t> Numel("numel",
                             llvm::cl::init(static_cast<int64_t>(1000)),
                             llvm::cl::desc("element number"));

namespace {

std::shared_ptr<yacl::link::Context> MakeLink(const std::string& parties,
                                              size_t rank) {
  yacl::link::ContextDesc lctx_desc;
  std::vector<std::string> hosts = absl::StrSplit(parties, ',');
  for (size_t rank = 0; rank < hosts.size(); rank++) {
    const auto id = fmt::format("party{}", rank);
    lctx_desc.parties.emplace_back(id, hosts[rank]);
  }
  auto lctx = yacl::link::FactoryBrpc().CreateContext(lctx_desc, rank);
  lctx->ConnectToMesh();
  return lctx;
}

std::shared_ptr<spu::SPUContext> MakeSPUContext(const std::string& parties,
                                                size_t rank) {
  auto lctx = MakeLink(parties, rank);
  spu::RuntimeConfig config;
  config.set_protocol(spu::ProtocolKind::SEMI2K);
  config.set_field(spu::FieldType::FM64);

  populateRuntimeConfig(config);

  return std::make_unique<spu::SPUContext>(config, lctx);
}

std::vector<std::shared_ptr<spu::SPUContext>> MakeContext(
    const std::string& parties) {
  std::vector<std::shared_ptr<spu::SPUContext>> ctxs;
  std::vector<std::future<std::shared_ptr<spu::SPUContext>>> rets;

  std::vector<std::string> hosts = absl::StrSplit(parties, ',');
  for (size_t rank = 0; rank < hosts.size(); rank++) {
    rets.push_back(std::async(MakeSPUContext, parties, rank));
  }
  for (size_t rank = 0; rank < hosts.size(); rank++) {
    ctxs.push_back(rets[rank].get());
  }
  return ctxs;
}

spu::Value GenRandomInput(spu::SPUContext* ctx) {
  auto iota = hlo::Iota(ctx, spu::DT_I64, Numel.getValue());
  auto rand_iota = hlo::Shuffle(ctx, {iota});
  return rand_iota[0];
}

spu::Value GenPrivPerm(spu::SPUContext* ctx, size_t rank) {
  auto perm = GenRandomInput(ctx);
  const auto perm_v = hal::_s2v(ctx, perm, rank);
  return perm_v;
}

spu::Value PreProcess(spu::SPUContext* ctx) {
  const auto input = GenRandomInput(ctx);
  return input;
}

void TestSP(spu::SPUContext* ctx, const spu::Value& in,
            const spu::Value& perm) {
  auto sorted = hal::_inv_perm_sv(ctx, in, perm).setDtype(spu::DT_I64);
}

void TestSecSort(spu::SPUContext* ctx, const spu::Value& in,
                 const spu::Value& perm) {
  auto sorted = hal::internal::_apply_inv_perm_ss(ctx, {in}, perm);
}

void TestSBK(spu::SPUContext* ctx, const spu::Value& in, int64_t valid_bits) {
  auto sorted = hal::internal::radix_sort(
      ctx, {in}, hal::SortDirection::Ascending, 1, valid_bits);
}

}  // namespace

int main(int argc, char** argv) {
  llvm::cl::ParseCommandLineOptions(argc, argv);
  auto ctxs = MakeContext(Parties.getValue());

  SPDLOG_INFO("Init spu cluster ...");
  std::vector<std::future<void>> init_rets;
  init_rets.reserve(ctxs.size());
  for (const auto& ctx : ctxs) {
    init_rets.push_back(std::async(spu::mpc::Factory::RegisterProtocol,
                                   ctx.get(), ctx->lctx()));
  }
  for (auto& ret : init_rets) {
    ret.get();
  }

  SPDLOG_INFO("Generating input data ...");
  std::vector<std::future<spu::Value>> in_;
  std::vector<spu::Value> in_vals;
  for (const auto& ctx : ctxs) {
    in_.push_back(std::async(PreProcess, ctx.get()));
  }
  for (auto& in : in_) {
    in_vals.push_back(in.get());
  }

  SPDLOG_INFO("Generating private permutation ...");
  std::vector<std::future<spu::Value>> perm_;
  std::vector<spu::Value> perm0_vals;
  for (const auto& ctx : ctxs) {
    perm_.push_back(std::async(GenPrivPerm, ctx.get(), 0));
  }
  for (auto& perm : perm_) {
    perm0_vals.push_back(perm.get());
  }
  perm_.clear();
  std::vector<spu::Value> perm1_vals;
  for (const auto& ctx : ctxs) {
    perm_.push_back(std::async(GenPrivPerm, ctx.get(), 1));
  }
  for (auto& perm : perm_) {
    perm1_vals.push_back(perm.get());
  }

  // Test SP
  SPDLOG_INFO("Testing SP ...");
  std::vector<std::future<void>> rets;
  rets.reserve(2);
  yacl::ElapsedTimer timer;
  timer.Restart();
  for (size_t i = 0; i < ctxs.size(); ++i) {
    rets.push_back(
        std::async(TestSP, ctxs[i].get(), in_vals[i], perm0_vals[i]));
  }

  for (auto& ret : rets) {
    ret.get();
  }
  SPDLOG_INFO("SP0 time {}", timer.CountMs());

  rets.clear();
  timer.Restart();
  for (size_t i = 0; i < ctxs.size(); ++i) {
    rets.push_back(
        std::async(TestSP, ctxs[i].get(), in_vals[i], perm1_vals[i]));
  }

  for (auto& ret : rets) {
    ret.get();
  }
  SPDLOG_INFO("SP1 time {}", timer.CountMs());

  // Test SecSort
  rets.clear();
  timer.Restart();
  for (size_t i = 0; i < ctxs.size(); ++i) {
    rets.push_back(
        std::async(TestSecSort, ctxs[i].get(), in_vals[i], in_vals[i]));
  }

  for (auto& ret : rets) {
    ret.get();
  }
  SPDLOG_INFO("SecSort time {}", timer.CountMs());

  // Test SBK
  rets.clear();
  timer.Restart();
  for (size_t i = 0; i < ctxs.size(); ++i) {
    rets.push_back(std::async(TestSBK, ctxs[i].get(), in_vals[i], -1));
  }

  for (auto& ret : rets) {
    ret.get();
  }
  SPDLOG_INFO("SortByKey time {}", timer.CountMs());

  rets.clear();
  timer.Restart();
  int64_t valid_bits = static_cast<int64_t>(std::log2(Numel.getValue()) + 1);
  for (size_t i = 0; i < ctxs.size(); ++i) {
    rets.push_back(std::async(TestSBK, ctxs[i].get(), in_vals[i], valid_bits));
  }

  for (auto& ret : rets) {
    ret.get();
  }
  SPDLOG_INFO("SortByKey with valid bits ({}) time {}", valid_bits,
              timer.CountMs());

  return 0;
}
