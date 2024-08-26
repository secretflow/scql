load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")
load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
)

configure_make(
    name = "gperftools_build",
    configure_options = [
        "--enable-shared=no",
        "--enable-frame-pointers",
        "--disable-libunwind",
        "--disable-dependency-tracking",
    ],
    env = {
        "AR": "",
    },
    lib_source = ":all_srcs",
    linkopts = ["-lpthread"],
    out_static_libs = ["libtcmalloc.a"],
    targets = ["-s -j4 install-libLTLIBRARIES install-perftoolsincludeHEADERS"],
)

cc_library(
    name = "gperftools",
    deps = [
        "gperftools_build",
    ],
)
