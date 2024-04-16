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

load("@rules_python//python:repositories.bzl", "py_repositories")

py_repositories()

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

load("@xla//:workspace4.bzl", "xla_workspace4")

xla_workspace4()

load("@xla//:workspace3.bzl", "xla_workspace3")

xla_workspace3()

load("@xla//:workspace2.bzl", "xla_workspace2")

xla_workspace2()

load("@xla//:workspace1.bzl", "xla_workspace1")

xla_workspace1()

load("@xla//:workspace0.bzl", "xla_workspace0")

xla_workspace0()

# grpc
load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

load("@rules_proto_grpc//:repositories.bzl", "rules_proto_grpc_repos", "rules_proto_grpc_toolchains")

rules_proto_grpc_toolchains()

rules_proto_grpc_repos()

#
# boost
#
load("@com_github_nelhage_rules_boost//:boost/boost.bzl", "boost_deps")

boost_deps()
