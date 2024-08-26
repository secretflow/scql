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

load("@spulib//bazel:spu.bzl", "spu_cmake_external")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "all_srcs",
    srcs = glob(
        ["**"],
        exclude = ["data/**"],
    ),
)

common_cache_entries = {
    "BUILD_SHARED_LIBS": "OFF",
    "BUILD_SHELL": "OFF",
    "BUILD_UNITTESTS": "OFF",
    "EXPORT_DLL_SYMBOLS": "OFF",
    "ENABLE_SANITIZER": "OFF",
    "ENABLE_UBSAN": "OFF",
    "OPENSSL_ROOT_DIR": "$EXT_BUILD_DEPS/openssl",
}

spu_cmake_external(
    name = "duckdb",
    build_args = ["-j 8"],
    cache_entries = common_cache_entries,
    env = {
        "CCACHE_DISABLE": "1",
    },
    lib_source = ":all_srcs",
    linkopts = [
        "-lm",
    ],
    out_static_libs = [
        "libduckdb_static.a",
        "libduckdb_pg_query.a",
        "libduckdb_re2.a",
        "libduckdb_miniz.a",
        "libduckdb_fmt.a",
        "libduckdb_utf8proc.a",
        "libduckdb_hyperloglog.a",
        "libduckdb_fastpforlib.a",
        "libduckdb_mbedtls.a",
        "libduckdb_fsst.a",
        "libparquet_extension.a",
        "libhttpfs_extension.a",
        "libduckdb_skiplistlib.a",
        "libduckdb_yyjson.a",
    ],
    deps = [
        "@com_github_openssl_openssl//:openssl",
    ],
)
