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
#   Apache Arrow library

# copied from https://github.com/tensorflow/io/blob/master/third_party/arrow.BUILD and made some changes

load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

exports_files(["LICENSE.txt"])

genrule(
    name = "arrow_util_config",
    srcs = ["cpp/src/arrow/util/config.h.cmake"],
    outs = ["cpp/src/arrow/util/config.h"],
    cmd = ("sed " +
           "-e 's/@ARROW_VERSION_MAJOR@/17/g' " +
           "-e 's/@ARROW_VERSION_MINOR@/0/g' " +
           "-e 's/@ARROW_VERSION_PATCH@/0/g' " +
           "-e 's/cmakedefine ARROW_USE_NATIVE_INT128/undef ARROW_USE_NATIVE_INT128/g' " +
           "-e 's/cmakedefine ARROW_WITH_OPENTELEMETRY/undef ARROW_WITH_OPENTELEMETRY/g' " +
           "-e 's/cmakedefine ARROW_GCS/undef ARROW_GCS/g' " +
           "-e 's/cmakedefine ARROW_JEMALLOC/undef ARROW_JEMALLOC/g' " +
           "-e 's/cmakedefine ARROW_JEMALLOC_VENDORED/undef ARROW_JEMALLOC_VENDORED/g' " +
           "-e 's/cmakedefine ARROW_AZURE/undef ARROW_AZURE/g' " +
           "-e 's/cmakedefine ARROW_MIMALLOC/undef ARROW_MIMALLOC/g' " +
           "-e 's/cmakedefine ARROW_HDFS/undef ARROW_HDFS/g' " +
           "-e 's/cmakedefine ARROW_USE_GLOG/undef ARROW_USE_GLOG/g' " +
           "-e 's/cmakedefine ARROW_WITH_RE2/undef ARROW_WITH_RE2/g' " +
           "-e 's/cmakedefine ARROW_WITH_UTF8PROC/undef ARROW_WITH_UTF8PROC/g' " +
           "-e 's/cmakedefine/define/g' " +
           "$< >$@"),
)

proto_library(
    name = "flight_proto",
    srcs = ["cpp/src/arrow/flight/Flight.proto"],
    deps = [
        "@com_google_protobuf//:descriptor_proto",
        "@com_google_protobuf//:timestamp_proto",
    ],
)

cc_proto_library(
    name = "flight_cc_proto",
    deps = [":flight_proto"],
)

cc_grpc_library(
    name = "flight_grpc_cc_proto",
    srcs = ["flight_proto"],
    grpc_only = True,
    deps = [":flight_cc_proto"],
)

proto_library(
    name = "flight_sql_proto",
    srcs = ["cpp/src/arrow/flight/sql/FlightSql.proto"],
    deps = ["@com_google_protobuf//:descriptor_proto"],
)

cc_proto_library(
    name = "flight_sql_cc_proto",
    deps = [":flight_sql_proto"],
)

cc_grpc_library(
    name = "flight_sql_grpc_cc_proto",
    srcs = ["flight_sql_proto"],
    grpc_only = True,
    deps = [":flight_sql_cc_proto"],
)

genrule(
    name = "parquet_version_h",
    srcs = ["cpp/src/parquet/parquet_version.h.in"],
    outs = ["cpp/src/parquet/parquet_version.h"],
    cmd = ("sed " +
           "-e 's/@PARQUET_VERSION_MAJOR@/1/g' " +
           "-e 's/@PARQUET_VERSION_MINOR@/5/g' " +
           "-e 's/@PARQUET_VERSION_PATCH@/1/g' " +
           "$< >$@"),
)

genrule(
    name = "arrow_version_h",
    srcs = ["cpp/src/arrow/util/config_internal.h.cmake"],
    outs = ["cpp/src/arrow/util/config_internal.h"],
    cmd = ("sed " +
           "-e 's/@ARROW_GIT_ID@/not_set/g' " +
           "-e 's/@ARROW_GIT_DESCRIPTION@/not_set/g' " +
           "$< >$@"),
)

cc_library(
    name = "arrow_vendored",
    srcs = glob([
        "cpp/src/arrow/vendored/datetime/*.h",
        "cpp/src/arrow/vendored/datetime/*.cpp",
        "cpp/src/arrow/vendored/pcg/pcg_uint128.hpp",
        "cpp/src/arrow/vendored/pcg/pcg_random.hpp",
        "cpp/src/arrow/vendored/pcg/pcg_extras.hpp",
        "cpp/src/arrow/vendored/uriparser/*.h",
        "cpp/src/arrow/vendored/uriparser/*.c",
        "cpp/src/arrow/vendored/double-conversion/*.h",
        "cpp/src/arrow/vendored/double-conversion/*.cc",
    ]),
    includes = [
        "cpp/src",
    ],
    visibility = ["//visibility:private"],
)

cc_library(
    name = "arrow",
    srcs = glob(
        [
            "cpp/src/arrow/*.cc",
            "cpp/src/arrow/c/*.cc",
            "cpp/src/arrow/array/*.cc",
            "cpp/src/arrow/csv/*.cc",
            "cpp/src/arrow/flight/**/*.cc",
            "cpp/src/arrow/flight/**/*.h",
            "cpp/src/arrow/extension/**/*.cc",
            "cpp/src/arrow/extension/**/*.h",
            "cpp/src/arrow/filesystem/*.cc",
            "cpp/src/arrow/filesystem/*.h",
            "cpp/src/arrow/io/*.cc",
            "cpp/src/arrow/ipc/*.cc",
            "cpp/src/arrow/json/*.cc",
            "cpp/src/arrow/tensor/*.cc",
            "cpp/src/arrow/compute/**/*.cc",
            "cpp/src/arrow/util/*.cc",
            # used by test files
            "cpp/src/arrow/testing/random.cc",
            "cpp/src/arrow/testing/gtest_util.cc",
            "cpp/src/arrow/vendored/optional.hpp",
            "cpp/src/arrow/vendored/string_view.hpp",
            "cpp/src/arrow/vendored/variant.hpp",
            "cpp/src/arrow/vendored/base64.cpp",
            "cpp/src/arrow/**/*.h",
            "cpp/src/parquet/**/*.h",
            "cpp/src/parquet/**/*.cc",
            "cpp/src/generated/*.h",
            "cpp/src/generated/*.cpp",
            "cpp/src/generated/parquet_types.tcc",
            "cpp/thirdparty/flatbuffers/include/flatbuffers/*.h",
        ],
        exclude = [
            "cpp/src/**/*_benchmark.cc",
            "cpp/src/**/*_main.cc",
            "cpp/src/**/*_nossl.cc",
            "cpp/src/**/*_test.cc",
            "cpp/src/**/test_*.h",
            "cpp/src/**/test_*.cc",
            "cpp/src/arrow/flight/sql/example/*.h",
            "cpp/src/arrow/flight/sql/example/*.cc",
            "cpp/src/arrow/flight/transport/ucx/*.cc",
            "cpp/src/arrow/flight/transport/ucx/*.h",
            "cpp/src/**/benchmark_util.h",
            "cpp/src/**/benchmark_util.cc",
            "cpp/src/**/*hdfs*.cc",
            "cpp/src/**/*fuzz*.cc",
            "cpp/src/arrow/memory_pool_jemalloc.cc",
            "cpp/src/**/file_to_stream.cc",
            "cpp/src/**/stream_to_file.cc",
            "cpp/src/arrow/dataset/file_orc*",
            "cpp/src/arrow/filesystem/gcsfs*.cc",
            "cpp/src/arrow/filesystem/azure*.cc",
            "cpp/src/arrow/filesystem/azure*.h",
            "cpp/src/arrow/filesystem/*_test_util.cc",
            "cpp/src/arrow/util/bpacking_avx2.cc",
            "cpp/src/arrow/util/bpacking_avx512.cc",
            "cpp/src/arrow/util/bpacking_neon.cc",
            "cpp/src/arrow/util/tracing_internal.cc",
            "cpp/src/arrow/compute/**/*_avx2.cc",
            "cpp/src/arrow/flight/try_compile/*.cc",
            "cpp/src/arrow/flight/try_compile/*.h",
            "cpp/src/arrow/flight/perf_server.cc",
            "cpp/src/arrow/flight/otel_logging.cc",
            "cpp/src/arrow/flight/otel_logging.h",
        ],
    ),
    hdrs = [
        # declare header from above genrule
        "cpp/src/arrow/util/config.h",
        "cpp/src/arrow/util/config_internal.h",
        "cpp/src/parquet/parquet_version.h",
    ],
    copts = [],
    defines = [
        "ARROW_STATIC",
        "ARROW_EXPORT=",
        "PARQUET_STATIC",
    ],
    includes = [
        "cpp/src",
        "cpp/src/arrow/vendored/xxhash",
        "cpp/thirdparty/flatbuffers/include",
    ],
    textual_hdrs = [
        "cpp/src/arrow/vendored/xxhash/xxhash.c",
    ],
    deps = [
        ":arrow_vendored",
        ":flight_cc_proto",
        ":flight_grpc_cc_proto",
        ":flight_sql_cc_proto",
        ":flight_sql_grpc_cc_proto",
        "@aws_sdk_cpp//:s3",
        "@boost//:multiprecision",
        "@brotli",
        "@bzip2",
        "@com_github_facebook_zstd//:zstd",
        "@com_github_gflags_gflags//:gflags",
        "@com_github_google_snappy//:snappy",
        "@com_github_grpc_grpc//:grpc++",
        "@com_github_grpc_grpc//:grpc++_reflection",
        "@com_github_lz4_lz4//:lz4",
        "@com_github_tencent_rapidjson//:rapidjson",
        "@com_github_xtensor_xsimd//:xsimd",
        "@com_google_double_conversion//:double-conversion",
        "@com_google_googletest//:gtest",
        "@org_apache_thrift//:thrift",
        "@zlib",
    ],
)
