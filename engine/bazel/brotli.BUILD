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

# Description:
#   Brotli library

# copied from https://github.com/tensorflow/io/blob/v0.25.0/third_party/brotli.BUILD

licenses(["notice"])  # MIT license

exports_files(["LICENSE"])

cc_library(
    name = "brotli",
    srcs = glob([
        "c/common/*.c",
        "c/common/*.h",
        "c/dec/*.c",
        "c/dec/*.h",
        "c/enc/*.c",
        "c/enc/*.h",
        "c/include/brotli/*.h",
    ]),
    hdrs = [],
    defines = [],
    includes = [
        "c/dec",
        "c/include",
    ],
    linkopts = [],
    visibility = ["//visibility:public"],
)
