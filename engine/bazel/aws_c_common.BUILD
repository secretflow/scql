# Copyright 2024 Ant Group Co., Ltd.
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
#   AWS C Common
package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

exports_files(["LICENSE"])

cc_library(
    name = "aws_c_common",
    srcs = glob([
        "include/aws/common/*.h",
        "include/aws/common/private/*.h",
        "source/*.c",
    ]) + select({
        "@bazel_tools//src/conditions:windows": glob([
            "source/windows/*.c",
        ]),
        "//conditions:default": glob([
            "source/posix/*.c",
        ]),
    }),
    hdrs = [
        "include/aws/common/config.h",
    ],
    defines = [],
    includes = [
        "include",
    ],
    textual_hdrs = glob([
        "include/**/*.inl",
    ]),
    deps = [],
)

genrule(
    name = "config_h",
    srcs = [
        "include/aws/common/config.h.in",
    ],
    outs = [
        "include/aws/common/config.h",
    ],
    cmd = "sed 's/cmakedefine/undef/g' $< > $@",
)
