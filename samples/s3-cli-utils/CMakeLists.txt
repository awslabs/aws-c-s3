project(s3-cli-utils C)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_INSTALL_PREFIX}/lib/cmake")

file(GLOB S3_CLI_UTIL_SRC
        "*.c"
        )

set(S3_CLI_UTIL_PROJECT_NAME s3-cli-utils)
add_library(${S3_CLI_UTIL_PROJECT_NAME} ${S3_CLI_UTIL_SRC})
aws_set_common_properties(${S3_CLI_UTIL_PROJECT_NAME})

target_include_directories(${S3_CLI_UTIL_PROJECT_NAME} PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)

target_link_libraries(${S3_CLI_UTIL_PROJECT_NAME} aws-c-s3)
