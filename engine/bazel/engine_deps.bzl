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

load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def engine_deps():
    _com_github_nelhage_rules_boost()
    _org_apache_arrow()
    _com_github_google_flatbuffers()
    _com_google_double_conversion()
    _com_github_tencent_rapidjson()
    _com_github_xtensor_xsimd()
    _bzip2()
    _brotli()
    _org_apache_thrift()
    _io_opentelemetry_cpp()
    _com_github_google_snappy()
    _com_github_lz4_lz4()
    _secretflow_deps()
    _com_mysql()
    _org_pocoproject_poco()
    _ncurses()
    _org_sqlite()
    _com_github_duckdb()
    _com_google_googleapis()

    _org_postgres()

def _secretflow_deps():
    #SPU_COMMIT = "4d9ed28a61f6fff20361cacb2073108adad57176"
    PSI_COMMIT = "65263edfa3255ffe17af6b30904b395366b5b661"
    #HEU_COMMIT = "afa15a0ad009cb5d5e40bd1dce885b9e4d472083"

    maybe(
        http_archive,
        name = "spulib",
        urls = [
            #"https://github.com/secretflow/spu/archive/%s.tar.gz" % SPU_COMMIT,
            "https://github.com/secretflow/spu/archive/refs/tags/0.7.0b0.tar.gz",
        ],
        #strip_prefix = "spu-%s" % SPU_COMMIT,
        strip_prefix = "spu-0.7.0b0",
        sha256 = "7387a4ea55f3b763d699d2c99268333763d38c6556279c525e78b62a9956f293",
    )
    maybe(
        http_archive,
        name = "psi",
        urls = [
            "https://github.com/secretflow/psi/archive/%s.tar.gz" % PSI_COMMIT,
        ],
        strip_prefix = "psi-%s" % PSI_COMMIT,
        sha256 = "8d1e42eaa435e6715c1b9dda38b1dec2245d499afe4f79c0da24602bf72cb72b",
    )

    maybe(
        http_archive,
        name = "com_alipay_sf_heu",
        urls = [
            #"https://github.com/secretflow/heu/archive/%s.tar.gz" % HEU_COMMIT,
            "https://github.com/secretflow/heu/archive/tags/v0.5.0b0.tar.gz",
        ],
        #strip_prefix = "heu-%s" % HEU_COMMIT,
        strip_prefix = "heu-tags-v0.5.0b0",
        sha256 = "fb62afb7506fb86ee43b9aea91a0fe260b547138794abd3647c1edae8eb5965a",
    )

def _org_apache_arrow():
    maybe(
        http_archive,
        name = "org_apache_arrow",
        urls = [
            "https://github.com/apache/arrow/archive/apache-arrow-10.0.0.tar.gz",
        ],
        patch_args = ["-p1"],
        patches = ["@scql//engine/bazel:patches/arrow.patch"],
        sha256 = "2852b21f93ee84185a9d838809c9a9c41bf6deca741bed1744e0fdba6cc19e3f",
        strip_prefix = "arrow-apache-arrow-10.0.0",
        build_file = "@scql//engine/bazel:arrow.BUILD",
    )

def _com_github_google_flatbuffers():
    maybe(
        http_archive,
        name = "com_github_google_flatbuffers",
        urls = [
            "https://github.com/google/flatbuffers/archive/refs/tags/v22.10.26.tar.gz",
        ],
        patch_args = ["-p1"],
        patches = ["@scql//engine/bazel:patches/flatbuffers.patch"],
        sha256 = "34f1820cfd78a3d92abc880fbb1a644c7fb31a71238995f4ed6b5915a1ad4e79",
        strip_prefix = "flatbuffers-22.10.26",
    )

def _com_google_double_conversion():
    maybe(
        http_archive,
        name = "com_google_double_conversion",
        sha256 = "a63ecb93182134ba4293fd5f22d6e08ca417caafa244afaa751cbfddf6415b13",
        strip_prefix = "double-conversion-3.1.5",
        build_file = "@scql//engine/bazel:double-conversion.BUILD",
        urls = [
            "https://github.com/google/double-conversion/archive/refs/tags/v3.1.5.tar.gz",
        ],
    )

def _com_github_tencent_rapidjson():
    maybe(
        http_archive,
        name = "com_github_tencent_rapidjson",
        urls = [
            "https://github.com/Tencent/rapidjson/archive/refs/tags/v1.1.0.tar.gz",
        ],
        sha256 = "bf7ced29704a1e696fbccf2a2b4ea068e7774fa37f6d7dd4039d0787f8bed98e",
        strip_prefix = "rapidjson-1.1.0",
        build_file = "@scql//engine/bazel:rapidjson.BUILD",
    )

def _com_github_xtensor_xsimd():
    maybe(
        http_archive,
        name = "com_github_xtensor_xsimd",
        urls = [
            "https://codeload.github.com/xtensor-stack/xsimd/tar.gz/refs/tags/8.1.0",
        ],
        sha256 = "d52551360d37709675237d2a0418e28f70995b5b7cdad7c674626bcfbbf48328",
        type = "tar.gz",
        strip_prefix = "xsimd-8.1.0",
        build_file = "@scql//engine/bazel:xsimd.BUILD",
    )

def _bzip2():
    maybe(
        http_archive,
        name = "bzip2",
        build_file = "@scql//engine/bazel:bzip2.BUILD",
        sha256 = "ab5a03176ee106d3f0fa90e381da478ddae405918153cca248e682cd0c4a2269",
        strip_prefix = "bzip2-1.0.8",
        urls = [
            "https://sourceware.org/pub/bzip2/bzip2-1.0.8.tar.gz",
        ],
    )

def _brotli():
    maybe(
        http_archive,
        name = "brotli",
        build_file = "@scql//engine/bazel:brotli.BUILD",
        sha256 = "f9e8d81d0405ba66d181529af42a3354f838c939095ff99930da6aa9cdf6fe46",
        strip_prefix = "brotli-1.0.9",
        urls = [
            "https://github.com/google/brotli/archive/refs/tags/v1.0.9.tar.gz",
        ],
    )

def _org_apache_thrift():
    maybe(
        http_archive,
        name = "org_apache_thrift",
        build_file = "@scql//engine/bazel:thrift.BUILD",
        sha256 = "5da60088e60984f4f0801deeea628d193c33cec621e78c8a43a5d8c4055f7ad9",
        strip_prefix = "thrift-0.13.0",
        urls = [
            "https://github.com/apache/thrift/archive/v0.13.0.tar.gz",
        ],
    )

def _io_opentelemetry_cpp():
    maybe(
        http_archive,
        name = "io_opentelemetry_cpp",
        urls = [
            "https://codeload.github.com/open-telemetry/opentelemetry-cpp/tar.gz/refs/tags/v1.3.0",
        ],
        sha256 = "6a4c43b9c9f753841ebc0fe2717325271f02e2a1d5ddd0b52735c35243629ab3",
        strip_prefix = "opentelemetry-cpp-1.3.0",
    )

def _com_github_google_snappy():
    maybe(
        http_archive,
        name = "com_github_google_snappy",
        urls = [
            "https://github.com/google/snappy/archive/refs/tags/1.1.9.tar.gz",
        ],
        sha256 = "75c1fbb3d618dd3a0483bff0e26d0a92b495bbe5059c8b4f1c962b478b6e06e7",
        strip_prefix = "snappy-1.1.9",
        build_file = "@scql//engine/bazel:snappy.BUILD",
    )

def _com_github_lz4_lz4():
    maybe(
        http_archive,
        name = "com_github_lz4_lz4",
        urls = [
            "https://codeload.github.com/lz4/lz4/tar.gz/refs/tags/v1.9.3",
        ],
        sha256 = "030644df4611007ff7dc962d981f390361e6c97a34e5cbc393ddfbe019ffe2c1",
        type = "tar.gz",
        strip_prefix = "lz4-1.9.3",
        build_file = "@scql//engine/bazel:lz4.BUILD",
    )

def _com_github_nelhage_rules_boost():
    # use boost 1.83
    RULES_BOOST_COMMIT = "cfa585b1b5843993b70aa52707266dc23b3282d0"
    maybe(
        http_archive,
        name = "com_github_nelhage_rules_boost",
        sha256 = "a7c42df432fae9db0587ff778d84f9dc46519d67a984eff8c79ae35e45f277c1",
        strip_prefix = "rules_boost-%s" % RULES_BOOST_COMMIT,
        patch_args = ["-p1"],
        patches = ["@scql//engine/bazel:patches/rules_boost.patch"],
        urls = [
            "https://github.com/nelhage/rules_boost/archive/%s.tar.gz" % RULES_BOOST_COMMIT,
        ],
    )

def _com_mysql():
    maybe(
        http_archive,
        name = "com_mysql",
        urls = [
            "https://github.com/mysql/mysql-server/archive/refs/tags/mysql-8.0.30.tar.gz",
        ],
        patch_args = ["-p1"],
        patches = ["@scql//engine/bazel:patches/mysql.patch"],
        sha256 = "e76636197f9cb764940ad8d800644841771def046ce6ae75c346181d5cdd879a",
        strip_prefix = "mysql-server-mysql-8.0.30",
        build_file = "@scql//engine/bazel:mysql.BUILD",
    )

def _org_postgres():
    maybe(
        http_archive,
        name = "org_postgres",
        urls = [
            "https://ftp.postgresql.org/pub/source/v15.2/postgresql-15.2.tar.gz",
        ],
        sha256 = "eccd208f3e7412ad7bc4c648ecc87e0aa514e02c24a48f71bf9e46910bf284ca",
        strip_prefix = "postgresql-15.2",
        build_file = "@scql//engine/bazel:postgres.BUILD",
    )

def _org_pocoproject_poco():
    maybe(
        http_archive,
        name = "org_pocoproject_poco",
        urls = [
            "https://github.com/pocoproject/poco/archive/refs/tags/poco-1.12.2-release.tar.gz",
        ],
        strip_prefix = "poco-poco-1.12.2-release",
        sha256 = "30442ccb097a0074133f699213a59d6f8c77db5b2c98a7c1ad9c5eeb3a2b06f3",
        build_file = "@scql//engine/bazel:poco.BUILD",
    )

def _ncurses():
    maybe(
        http_archive,
        name = "ncurses",
        urls = [
            "https://ftp.gnu.org/pub/gnu/ncurses/ncurses-6.3.tar.gz",
        ],
        sha256 = "97fc51ac2b085d4cde31ef4d2c3122c21abc217e9090a43a30fc5ec21684e059",
        strip_prefix = "ncurses-6.3",
        build_file = "@scql//engine/bazel:ncurses.BUILD",
    )

def _org_sqlite():
    maybe(
        http_archive,
        name = "org_sqlite",
        urls = [
            "https://www.sqlite.org/2020/sqlite-amalgamation-3320200.zip",
        ],
        sha256 = "7e1ebd182a61682f94b67df24c3e6563ace182126139315b659f25511e2d0b5d",
        strip_prefix = "sqlite-amalgamation-3320200",
        build_file = "@scql//engine/bazel:sqlite3.BUILD",
    )

def _com_github_duckdb():
    maybe(
        http_archive,
        name = "com_github_duckdb",
        urls = [
            "https://github.com/duckdb/duckdb/archive/refs/tags/v0.9.2.tar.gz",
        ],
        patch_args = ["-p1"],
        patches = ["@scql//engine/bazel:patches/duckdb.patch"],
        sha256 = "afff7bd925a98dc2af4039b8ab2159b0705cbf5e0ee05d97f7bb8dce5f880dc2",
        strip_prefix = "duckdb-0.9.2",
        build_file = "@scql//engine/bazel:duckdb.BUILD",
    )

def _com_google_googleapis():
    maybe(
        http_archive,
        name = "googleapis",
        urls = [
            "https://github.com/googleapis/googleapis/archive/40b951b9d77aca86f017c74750f30df0e090ed68.tar.gz",
        ],
        strip_prefix = "googleapis-40b951b9d77aca86f017c74750f30df0e090ed68",
        sha256 = "da9aa10ba661e95ee031044e2d17a399d0cbd47488163fb13ac6a6f14b36ba36",
    )
