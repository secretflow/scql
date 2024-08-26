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
    name = "mysqlclient",
    build_args = ["-j 8"],
    cache_entries = {
        "WITHOUT_SERVER": "ON",
        "INSTALL_PRIV_LIBDIR": "$EXT_BUILD_DEPS",
        "WITH_SSL": "$EXT_BUILD_DEPS/openssl",  # MySQL 8.0.30 and later could use OpenSSL3
        "WITH_UNIT_TESTS": "OFF",
        "WITH_PROTOBUF": "system",
        "WITH_BOOST": "$EXT_BUILD_DEPS/include",
        "CURSES_LIBRARY": "$EXT_BUILD_DEPS/ncurses/lib/libcurses.a",
        "CURSES_INCLUDE_PATH": "$EXT_BUILD_DEPS/ncurses/include/",
        "INSTALL_INCLUDEDIR": "include/mysql",  # poco data needs <mysql/mysql.h>
        "CMAKE_C_COMPILER": "gcc",
        "CMAKE_CXX_COMPILER": "g++",
    },
    install = False,
    lib_source = ":all_srcs",
    linkopts = [
        "-lpthread",
    ],
    out_static_libs = [
        "libmysqlclient.a",
    ],
    # fix install_args can't specify directory to install
    postfix_script = "cmake --install include --config Release --component Development --prefix $$INSTALLDIR$$ && cp -p archive_output_directory/libmysqlclient.a $$INSTALLDIR$$/lib/",
    targets = [
        "mysqlclient",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@boost//:endian",
        "@boost//:geometry",
        "@boost//:graph",
        "@boost//:multiprecision",
        "@boost//:spirit",
        "@com_github_openssl_openssl//:openssl",
        "@com_google_protobuf//:protobuf",
        "@ncurses",
        "@zlib",
    ],
)
