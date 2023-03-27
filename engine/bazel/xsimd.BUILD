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

# copied from https://github.com/tensorflow/io/blob/v0.25.0/third_party/xsimd.BUILD

package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # BSD 3-Clause

exports_files(["LICENSE"])

cc_library(
    name = "xsimd",
    srcs = [],
    hdrs = glob(
        [
            "include/xsimd/*.hpp",
            "include/xsimd/config/*.hpp",
            "include/xsimd/math/*.hpp",
            "include/xsimd/memory/*.hpp",
            "include/xsimd/stl/*.hpp",
            "include/xsimd/types/*.hpp",
        ],
        exclude = [
        ],
    ),
    copts = [],
    defines = [],
    includes = [
        "include",
    ],
    linkopts = [],
    visibility = ["//visibility:public"],
    deps = [
    ],
)
