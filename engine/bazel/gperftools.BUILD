# Copyright 2025 Ant Group Co., Ltd.
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

load("@rules_cc//cc:defs.bzl", "cc_library")
load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
)

configure_make(
    name = "gperftools_build",
    configure_options = [
        "--enable-shared=no",
        "--enable-frame-pointers",
        "--disable-libunwind",
        "--disable-dependency-tracking",
    ],
    env = {
        "AR": "",
    },
    lib_source = ":all_srcs",
    linkopts = ["-lpthread"],
    out_static_libs = ["libtcmalloc.a"],
    targets = ["-s -j4 install-libLTLIBRARIES install-perftoolsincludeHEADERS"],
)

cc_library(
    name = "gperftools",
    deps = [
        "gperftools_build",
    ],
)
