# Copyright 2017, OpenCensus Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# copied from: https://github.com/census-instrumentation/opencensus-cpp/blob/master/WORKSPACE
#         and: https://github.com/open-telemetry/opentelemetry-cpp/blob/main/bazel/curl.BUILD

load("@rules_cc//cc:defs.bzl", "cc_library")
load("@scql//engine/bazel:curl.bzl", "CURL_COPTS")

licenses(["notice"])  # MIT/X derivative license

package(features = ["no_copts_tokenization"])

config_setting(
    name = "windows",
    constraint_values = [
        "@platforms//os:windows",
    ],
    visibility = ["//visibility:private"],
)

config_setting(
    name = "osx",
    constraint_values = [
        "@platforms//os:osx",
    ],
    visibility = ["//visibility:private"],
)

cc_library(
    name = "curl",
    srcs = glob([
        "lib/**/*.c",
    ]),
    hdrs = glob([
        "include/curl/*.h",
        "lib/**/*.h",
    ]),
    copts = CURL_COPTS + [
        '-DOS="os"',
    ],
    defines = ["CURL_STATICLIB"],
    includes = [
        "include/",
        "lib/",
    ],
    linkopts = select({
        "//:windows": [
            "-DEFAULTLIB:ws2_32.lib",
            "-DEFAULTLIB:advapi32.lib",
            "-DEFAULTLIB:crypt32.lib",
            "-DEFAULTLIB:Normaliz.lib",
        ],
        "//:osx": [
            "-framework SystemConfiguration",
            "-lpthread",
        ],
        "//conditions:default": [
            "-lpthread",
        ],
    }),
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_openssl_openssl//:openssl",
    ],
)
