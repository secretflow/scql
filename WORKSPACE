# Copyright 2023 Ant Group Co., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

workspace(name = "scql")

load("//bazel:repositories.bzl", "scql_deps")

scql_deps()

#
# spulib
#
load("@spulib//bazel:repositories.bzl", "spu_deps")

spu_deps()

#
# yacl
#
load("@yacl//bazel:repositories.bzl", "yacl_deps")

yacl_deps()

#
# psi
#
load("@psi//bazel:repositories.bzl", "psi_deps")

psi_deps()

# grpc
load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

# Not mentioned in official docs... mentioned here https://github.com/grpc/grpc/issues/20511
load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")

grpc_extra_deps()

#
# heu
#
load("@com_alipay_sf_heu//third_party/bazel_cpp:repositories.bzl", "heu_cpp_deps")

heu_cpp_deps()

load(
    "@rules_foreign_cc//foreign_cc:repositories.bzl",
    "rules_foreign_cc_dependencies",
)

rules_foreign_cc_dependencies(
    register_built_tools = False,
    register_default_tools = False,
    register_preinstalled_tools = True,
)

load("@xla//third_party/llvm:workspace.bzl", llvm = "repo")

llvm("llvm-raw")

load("@xla//third_party/llvm:setup.bzl", "llvm_setup")

llvm_setup("llvm-project")

#
# boost
#
load("@com_github_nelhage_rules_boost//:boost/boost.bzl", "boost_deps")

boost_deps()




