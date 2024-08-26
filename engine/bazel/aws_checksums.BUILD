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
#   AWS CheckSums
package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

exports_files(["LICENSE"])

cc_library(
    name = "aws_checksums",
    srcs = glob([
        "include/aws/checksums/*.h",
        "include/aws/checksums/private/*.h",
        "source/*.c",
    ]) + [
        "crc_hw.c",
    ],
    hdrs = [],
    defines = [],
    includes = [
        "include",
    ],
    deps = [],
)

genrule(
    name = "crc_hw_c",
    outs = ["crc_hw.c"],
    cmd = "\n".join([
        "cat <<'EOF' >$@",
        "#include <aws/checksums/private/cpuid.h>",
        "#include <aws/checksums/private/crc_priv.h>",
        "int aws_checksums_do_cpu_id(int32_t *cpuid) {",
        "    return 0;",
        "}",
        "uint32_t aws_checksums_crc32c_hw(const uint8_t *input, int length, uint32_t previousCrc32) {",
        "  return aws_checksums_crc32c_sw(input, length, previousCrc32);",
        "}",
        "EOF",
    ]),
)
