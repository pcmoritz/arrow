java_library(
    name = "org_apache_arrow_arrow_plasma",
    srcs = glob(["plasma/src/main/java/**/*.java"]),
    data = [":plasma_client_java"],
    visibility = ["//visibility:public"],
    deps = [
        "@maven//:org_slf4j_slf4j_api",
    ],
)

java_binary(
    name = "org_apache_arrow_arrow_plasma_test",
    srcs = ["plasma/src/test/java/org/apache/arrow/plasma/PlasmaClientTest.java"],
    main_class = "org.apache.arrow.plasma.PlasmaClientTest",
    visibility = ["//visibility:public"],
    deps = [
        ":org_apache_arrow_arrow_plasma",
        "@maven//:junit_junit",
    ],
)

filegroup(
    name = "plasma_client_java",
    srcs = select({
        "@bazel_tools//src/conditions:darwin": ["//cpp/src:libplasma_java.dylib"],
        "//conditions:default": ["//cpp/src:libplasma_java.so"],
    }),
    visibility = ["//visibility:public"],
)
