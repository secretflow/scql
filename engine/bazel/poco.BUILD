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

filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
)

spu_cmake_external(
    name = "poco",
    cache_entries = {
        "ENABLE_DATA_SQLITE": "ON",
        "ENABLE_DATA_MYSQL": "ON",
        # disable not required components to speed up the build process
        "ENABLE_DATA_ODBC": "OFF",
        "ENABLE_DATA_POSTGRESQL": "ON",
        "ENABLE_ACTIVERECORD": "OFF",
        "ENABLE_ACTIVERECORD_COMPILER": "OFF",
        "ENABLE_ENCODINGS": "OFF",
        "ENABLE_XML": "OFF",
        "ENABLE_JSON": "OFF",
        "ENABLE_MONGODB": "OFF",
        "ENABLE_REDIS": "OFF",
        "ENABLE_PROMETHEUS": "OFF",
        "ENABLE_UTIL": "OFF",
        "ENABLE_NET": "OFF",
        "ENABLE_ZIP": "OFF",
        "ENABLE_TESTS": "OFF",
        "ENABLE_APACHECONNECTOR": "OFF",
        "CMAKE_FIND_DEBUG_MODE": "OFF",
        "BUILD_SHARED_LIBS": "OFF",
        "OPENSSL_ROOT_DIR": "$EXT_BUILD_DEPS/openssl",
        "MYSQL_ROOT_INCLUDE_DIRS": "$EXT_BUILD_DEPS/mysqlclient/include",
        "MYSQL_ROOT_LIBRARY_DIRS": "$EXT_BUILD_DEPS/mysqlclient/lib",
        "PostgreSQL_LIBRARY": "$EXT_BUILD_DEPS/postgres/lib",
        "PostgreSQL_INCLUDE_DIR": "$EXT_BUILD_DEPS/postgres/include",
        "CMAKE_BUILD_TYPE": "Release",
    },
    generate_crosstool_file = False,
    lib_source = ":all_srcs",
    linkopts = select({
        "@platforms//os:osx": [],
        "//conditions:default": [
            "-lrt",
        ],
    }),
    out_lib_dir = "lib",
    out_static_libs = [
        "libPocoFoundation.a",
        "libPocoData.a",
        "libPocoDataMySQL.a",
        "libPocoDataSQLite.a",
        "libPocoDataPostgreSQL.a",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@com_mysql//:mysqlclient",
        "@org_postgres//:postgres",
        "@org_sqlite//:sqlite3",
    ],
)
