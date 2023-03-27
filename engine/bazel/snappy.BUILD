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

load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
)

cmake(
    name = "snappy",
    cache_entries = {
        "SNAPPY_BUILD_TESTS": "OFF",
        "SNAPPY_BUILD_BENCHMARKS": "OFF",
        "CMAKE_INSTALL_LIBDIR": "lib",
    },
    generate_crosstool_file = False,
    install_args = [
        "--prefix $${INSTALLDIR}",
    ],
    lib_source = ":all_srcs",
    out_static_libs = [
        "libsnappy.a",
    ],
)
