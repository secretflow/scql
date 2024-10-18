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
    _com_github_gperftools_gperftools()
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
    _com_github_jupp0r_prometheus_cpp()

    _org_postgres()

    # aws s3
    _com_github_curl()
    _com_aws_c_common()
    _com_aws_checksums()
    _com_aws_c_event_stream()
    _com_aws_sdk()

def _secretflow_deps():
    # SPU_COMMIT = "3ccae529b70b37f7312e1171eab7e5acc16ff136"
    # PSI_COMMIT = "47aaeb9c9ccd92deb8c150b2d55302ac0070f723"
    # HEU_COMMIT = "afa15a0ad009cb5d5e40bd1dce885b9e4d472083"
    # KUSCIA_COMMIT = "1979d1f4f17db5c2bd6c57be7a690e88fa9ce7ed"

    maybe(
        http_archive,
        name = "spulib",
        urls = [
            # "https://github.com/secretflow/spu/archive/%s.tar.gz" % SPU_COMMIT,
            "https://github.com/secretflow/spu/archive/refs/tags/0.9.3dev20240821.tar.gz",
        ],
        # strip_prefix = "spu-%s" % SPU_COMMIT,
        strip_prefix = "spu-0.9.3dev20240821",
        sha256 = "c04e6a6eb61e7eb16a267a499020adc6867ecf7e640464bbdec986023b0edecb",
    )

    maybe(
        http_archive,
        name = "psi",
        urls = [
            #"https://github.com/secretflow/psi/archive/%s.tar.gz" % PSI_COMMIT,
            "https://github.com/secretflow/psi/archive/refs/tags/v0.5.0.dev241016.tar.gz",
        ],
        # strip_prefix = "psi-%s" % PSI_COMMIT,
        strip_prefix = "psi-0.5.0.dev241016",
        sha256 = "1672e4284f819c40e34c65b0d5b1dfe4cc959b81d6f63daef7b39f7eb8d742e2",
    )

    maybe(
        http_archive,
        name = "com_alipay_sf_heu",
        urls = [
            # "https://github.com/secretflow/heu/archive/%s.tar.gz" % HEU_COMMIT,
            "https://github.com/secretflow/heu/archive/refs/tags/0.6.0.dev20240529.tar.gz",
        ],
        # strip_prefix = "heu-%s" % HEU_COMMIT,
        strip_prefix = "heu-0.6.0.dev20240529",
        sha256 = "1430a17385286c11792e78c7b038860f12b6a0716e8fe8a547fb4626be60195f",
    )

    maybe(
        http_archive,
        name = "kuscia",
        urls = [
            # "https://github.com/secretflow/kuscia/archive/%s.tar.gz" % KUSCIA_COMMIT,
            "https://github.com/secretflow/kuscia/archive/refs/tags/v0.7.0b0.tar.gz",
        ],
        strip_prefix = "kuscia-0.7.0b0",
        sha256 = "76e396f9b148ec741e3c938d0a54ce9e91709466254d2f6effc8a4d50a77ff97",
    )

def _org_apache_arrow():
    maybe(
        http_archive,
        name = "org_apache_arrow",
        urls = [
            "https://github.com/apache/arrow/archive/apache-arrow-17.0.0.tar.gz",
        ],
        patch_args = ["-p1"],
        patches = ["@scql//engine/bazel:patches/arrow.patch"],
        sha256 = "8379554d89f19f2c8db63620721cabade62541f47a4e706dfb0a401f05a713ef",
        strip_prefix = "arrow-apache-arrow-17.0.0",
        build_file = "@scql//engine/bazel:arrow.BUILD",
    )

def _com_github_gperftools_gperftools():
    maybe(
        http_archive,
        name = "com_github_gperftools_gperftools",
        type = "tar.gz",
        strip_prefix = "gperftools-2.15",
        sha256 = "c69fef855628c81ef56f12e3c58f2b7ce1f326c0a1fe783e5cae0b88cbbe9a80",
        urls = [
            "https://github.com/gperftools/gperftools/releases/download/gperftools-2.15/gperftools-2.15.tar.gz",
        ],
        build_file = "@scql//engine/bazel:gperftools.BUILD",
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
            "https://github.com/duckdb/duckdb/archive/refs/tags/v1.0.0.tar.gz",
        ],
        patch_args = ["-p1"],
        patches = ["@scql//engine/bazel:patches/duckdb.patch"],
        sha256 = "04e472e646f5cadd0a3f877a143610674b0d2bcf9f4102203ac3c3d02f1c5f26",
        strip_prefix = "duckdb-1.0.0",
        build_file = "@scql//engine/bazel:duckdb.BUILD",
    )

def _com_google_googleapis():
    maybe(
        http_archive,
        name = "googleapis",
        urls = [
            "https://github.com/googleapis/googleapis/archive/fea22b1d9f27f86ef355c1d0dba00e0791a08a19.tar.gz",
        ],
        strip_prefix = "googleapis-fea22b1d9f27f86ef355c1d0dba00e0791a08a19",
        sha256 = "957ef432cdedbace1621bb023e6d8637ecbaa78856b3fc6e299f9b277ae990ff",
    )

def _com_github_jupp0r_prometheus_cpp():
    maybe(
        http_archive,
        name = "com_github_jupp0r_prometheus_cpp",
        urls = [
            "https://github.com/jupp0r/prometheus-cpp/archive/v1.2.1.tar.gz",
        ],
        strip_prefix = "prometheus-cpp-1.2.1",
        sha256 = "190734c4d8d0644c2af327ff8b5ef86cd7ea9074a48d777112394f558dd014f7",
    )

def _com_github_curl():
    maybe(
        http_archive,
        name = "com_github_curl",
        sha256 = "01ae0c123dee45b01bbaef94c0bc00ed2aec89cb2ee0fd598e0d302a6b5e0a98",
        strip_prefix = "curl-7.69.1",
        urls = [
            "https://github.com/curl/curl/releases/download/curl-7_69_1/curl-7.69.1.tar.gz",
            "https://curl.haxx.se/download/curl-7.69.1.tar.gz",
        ],
        build_file = "@scql//engine/bazel:curl.BUILD",
    )

def _com_aws_c_common():
    maybe(
        http_archive,
        name = "aws_c_common",
        urls = [
            "https://github.com/awslabs/aws-c-common/archive/v0.4.29.tar.gz",
        ],
        sha256 = "01c2a58553a37b3aa5914d9e0bf7bf14507ff4937bc5872a678892ca20fcae1f",
        strip_prefix = "aws-c-common-0.4.29",
        build_file = "@scql//engine/bazel:aws_c_common.BUILD",
    )

def _com_aws_checksums():
    maybe(
        http_archive,
        name = "aws_checksums",
        urls = [
            "https://github.com/awslabs/aws-checksums/archive/v0.1.5.tar.gz",
        ],
        sha256 = "6e6bed6f75cf54006b6bafb01b3b96df19605572131a2260fddaf0e87949ced0",
        strip_prefix = "aws-checksums-0.1.5",
        build_file = "@scql//engine/bazel:aws_checksums.BUILD",
    )

def _com_aws_c_event_stream():
    maybe(
        http_archive,
        name = "aws_c_event_stream",
        urls = [
            "https://github.com/awslabs/aws-c-event-stream/archive/v0.1.4.tar.gz",
        ],
        sha256 = "31d880d1c868d3f3df1e1f4b45e56ac73724a4dc3449d04d47fc0746f6f077b6",
        strip_prefix = "aws-c-event-stream-0.1.4",
        build_file = "@scql//engine/bazel:aws_c_event_stream.BUILD",
    )

def _com_aws_sdk():
    maybe(
        http_archive,
        name = "aws_sdk_cpp",
        strip_prefix = "aws-sdk-cpp-1.7.336",
        urls = [
            "https://github.com/aws/aws-sdk-cpp/archive/1.7.336.tar.gz",
        ],
        sha256 = "758174f9788fed6cc1e266bcecb20bf738bd5ef1c3d646131c9ed15c2d6c5720",
        build_file = "@scql//engine/bazel:aws_sdk_cpp.BUILD",
        patch_cmds = [
            """sed -i.bak 's/UUID::RandomUUID/Aws::Utils::UUID::RandomUUID/g' aws-cpp-sdk-core/source/client/AWSClient.cpp""",
            """sed -i.bak 's/__attribute__((visibility("default")))//g' aws-cpp-sdk-core/include/aws/core/external/tinyxml2/tinyxml2.h """,
        ],
    )
