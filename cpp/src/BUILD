load("@com_github_google_flatbuffers//:build_defs.bzl", "flatbuffer_cc_library")

package(
    default_visibility = ["//visibility:public"],
)

COPTS = ["-DARROW_USE_GLOG"]

cc_library(
    name = "arrow",
    srcs = [
        "arrow/buffer.cc",
        "arrow/io/interfaces.cc",
        "arrow/memory_pool.cc",
        "arrow/status.cc",
        "arrow/util/io-util.cc",
        "arrow/util/logging.cc",
        "arrow/util/memory.cc",
        "arrow/util/string_builder.cc",
        "arrow/util/thread-pool.cc",
    ],
    hdrs = [
        "arrow/buffer.h",
        "arrow/io/interfaces.h",
        "arrow/memory_pool.h",
        "arrow/status.h",
        "arrow/testing/gtest_util.h",
        "arrow/util/bit-util.h",
        "arrow/util/io-util.h",
        "arrow/util/logging.h",
        "arrow/util/macros.h",
        "arrow/util/memory.h",
        "arrow/util/stl.h",
        "arrow/util/string.h",
        "arrow/util/string_builder.h",
        "arrow/util/string_view.h",
        "arrow/util/thread-pool.h",
        "arrow/util/type_traits.h",
        "arrow/util/ubsan.h",
        "arrow/util/visibility.h",
        "arrow/util/windows_compatibility.h",
        "arrow/vendored/string_view.hpp",
        "arrow/vendored/xxhash/xxhash.c",
        "arrow/vendored/xxhash/xxhash.h",
    ],
    copts = COPTS,
    strip_include_prefix = ".",
    visibility = ["//visibility:public"],
    deps = [
        "@boost//:filesystem",
        "@com_github_google_glog//:glog",
    ],
)

cc_library(
    name = "plasma_client",
    srcs = [
        "plasma/client.cc",
        "plasma/common.cc",
        "plasma/fling.cc",
        "plasma/io.cc",
        "plasma/malloc.cc",
        "plasma/plasma.cc",
        "plasma/protocol.cc",
    ],
    hdrs = [
        "plasma/client.h",
        "plasma/common.h",
        "plasma/common_generated.h",
        "plasma/compat.h",
        "plasma/external_store.h",
        "plasma/fling.h",
        "plasma/io.h",
        "plasma/malloc.h",
        "plasma/plasma.h",
        "plasma/plasma_generated.h",
        "plasma/protocol.h",
    ],
    copts = COPTS,
    strip_include_prefix = ".",
    visibility = ["//visibility:public"],
    deps = [
        ":arrow",
        ":common_fbs",
        ":plasma_fbs",
        "@com_github_google_glog//:glog",
    ],
)

cc_library(
    name = "plasma_lib",
    srcs = [
        "plasma/dlmalloc.cc",
        "plasma/events.cc",
        "plasma/eviction_policy.cc",
        "plasma/external_store.cc",
        "plasma/plasma_allocator.cc",
        "plasma/thirdparty/ae/ae.c",
    ],
    hdrs = [
        "plasma/events.h",
        "plasma/eviction_policy.h",
        "plasma/external_store.h",
        "plasma/plasma_allocator.h",
        "plasma/store.h",
        "plasma/test-util.h",
        "plasma/thirdparty/ae/ae.h",
        "plasma/thirdparty/ae/ae_epoll.c",
        "plasma/thirdparty/ae/ae_evport.c",
        "plasma/thirdparty/ae/ae_kqueue.c",
        "plasma/thirdparty/ae/ae_select.c",
        "plasma/thirdparty/ae/config.h",
        "plasma/thirdparty/ae/zmalloc.h",
        "plasma/thirdparty/dlmalloc.c",
    ],
    copts = COPTS,
    strip_include_prefix = ".",
    deps = [
        ":plasma_client",
        "@com_github_google_glog//:glog",
    ],
)

cc_binary(
    name = "libplasma_java.so",
    srcs = [
        "plasma/lib/java/org_apache_arrow_plasma_PlasmaClientJNI.cc",
        "plasma/lib/java/org_apache_arrow_plasma_PlasmaClientJNI.h",
        ":jni.h",
        ":jni_md.h",
    ],
    includes = [
        ".",
    ],
    linkshared = 1,
    linkstatic = 1,
    deps = [":plasma_client"],
    visibility = ["//visibility:public"],
)

genrule(
    name = "copy_jni_h",
    srcs = ["@bazel_tools//tools/jdk:jni_header"],
    outs = ["jni.h"],
    cmd = "cp -f $< $@",
)

genrule(
    name = "copy_jni_md_h",
    srcs = select({
        "@bazel_tools//src/conditions:windows": ["@bazel_tools//tools/jdk:jni_md_header-windows"],
        "@bazel_tools//src/conditions:darwin": ["@bazel_tools//tools/jdk:jni_md_header-darwin"],
        "//conditions:default": ["@bazel_tools//tools/jdk:jni_md_header-linux"],
    }),
    outs = ["jni_md.h"],
    cmd = "cp -f $< $@",
)

genrule(
    name = "plasma-jni-darwin-compat",
    srcs = [":libplasma_java.so"],
    outs = ["libplasma_java.dylib"],
    cmd = "cp $< $@",
    output_to_bindir = 1,
    visibility = ["//visibility:public"],
)

cc_binary(
    name = "plasma_store_server",
    srcs = [
        "plasma/store.cc",
    ],
    visibility = ["//visibility:public"],
    deps = [":plasma_lib"],
)

cc_test(
    name = "client_tests",
    srcs = ["plasma/test/client_tests.cc"],
    copts = COPTS,
    deps = [
        ":plasma_lib",
        ":plasma_store_server",
        "@com_google_googletest//:gtest_main",
    ],
)

FLATC_ARGS = [
    "--gen-object-api",
    "--gen-mutable",
    "--scoped-enums",
]

flatbuffer_cc_library(
    name = "common_fbs",
    srcs = ["plasma/format/common.fbs"],
    flatc_args = FLATC_ARGS,
    out_prefix = "plasma/",
)

flatbuffer_cc_library(
    name = "plasma_fbs",
    srcs = ["plasma/format/plasma.fbs"],
    flatc_args = FLATC_ARGS,
    includes = ["plasma/format/common.fbs"],
    out_prefix = "plasma/",
)
