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
#   Apache Thrift library

# copied from https://github.com/tensorflow/io/blob/v0.25.0/third_party/thrift.BUILD

package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

exports_files(["LICENSE"])

cc_library(
    name = "thrift",
    srcs = glob([
        "lib/cpp/src/thrift/**/*.h",
    ]) + [
        "lib/cpp/src/thrift/protocol/TProtocol.cpp",
        "lib/cpp/src/thrift/transport/TBufferTransports.cpp",
        "lib/cpp/src/thrift/transport/TTransportException.cpp",
    ],
    hdrs = [
        "compiler/cpp/src/thrift/version.h",
        "lib/cpp/src/thrift/config.h",
    ],
    includes = [
        "lib/cpp/src",
    ],
    textual_hdrs = [
        "lib/cpp/src/thrift/protocol/TBinaryProtocol.tcc",
        "lib/cpp/src/thrift/protocol/TCompactProtocol.tcc",
    ],
    deps = [
        "@boost//:units",
    ],
)

genrule(
    name = "version_h",
    srcs = [
        "compiler/cpp/src/thrift/version.h.in",
    ],
    outs = [
        "compiler/cpp/src/thrift/version.h",
    ],
    cmd = "sed 's/@PACKAGE_VERSION@/0.12.0/g' $< > $@",
)

genrule(
    name = "config_h",
    srcs = ["build/cmake/config.h.in"],
    outs = ["lib/cpp/src/thrift/config.h"],
    cmd = ("sed " +
           "-e 's/cmakedefine/define/g' " +
           "-e 's/$${PACKAGE}/thrift/g' " +
           "-e 's/$${PACKAGE_BUGREPORT}//g' " +
           "-e 's/$${PACKAGE_NAME}/thrift/g' " +
           "-e 's/$${PACKAGE_TARNAME}/thrift/g' " +
           "-e 's/$${PACKAGE_URL}//g' " +
           "-e 's/$${PACKAGE_VERSION}/0.12.0/g' " +
           "-e 's/$${PACKAGE_STRING}/thrift 0.12.0/g' " +
           "$< >$@"),
)
