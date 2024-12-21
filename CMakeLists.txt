
cmake_minimum_required(VERSION 3.9...3.31)
project(aws-c-s3 C)

option(ASSERT_LOCK_HELD "Enable ASSERT_SYNCED_DATA_LOCK_HELD for checking thread issue" OFF)
option(ENABLE_MOCK_SERVER_TESTS "Whether to run the integration tests that rely on pre-configured mock server" OFF)
option(ENABLE_MRAP_TESTS "Whether to run the integration tests that rely on pre-configured multi-region access point" OFF)
option(AWS_ENABLE_S3_ENDPOINT_RESOLVER "Whether to include the s3 endpoint resolver and related config files" OFF)

if (ASSERT_LOCK_HELD)
    add_definitions(-DASSERT_LOCK_HELD)
endif()

if (NOT IN_SOURCE_BUILD)
    # this is required so we can use aws-c-common's CMake modules
    find_package(aws-c-common REQUIRED)
endif()

include(AwsCFlags)
include(AwsCheckHeaders)
include(AwsSharedLibSetup)
include(AwsSanitizers)
include(AwsFindPackage)
include(GNUInstallDirs)

file(GLOB AWS_S3_ROOT_HEADERS
        "include/aws/s3/*.h"
        )

file(GLOB AWS_S3_PRIVATE_HEADERS
        "include/aws/s3/private/*.h"
        )

file(GLOB AWS_S3_ROOT_SRC
        "source/*.c"
        )
file(GLOB AWS_S3_ENDPOINT_RESOLVER_SRC
        "source/s3_endpoint_resolver/*.c"
)

if (WIN32)
    if (MSVC)
        source_group("Header Files\\aws\\s3" FILES ${AWS_S3_HEADERS})
        source_group("Source Files" FILES ${AWS_S3_SRC})
    endif ()
endif()

file(GLOB S3_HEADERS
        ${AWS_S3_ROOT_HEADERS}
        ${AWS_S3_PRIVATE_HEADERS}
        ${AWS_S3_EXTERNAL_HEADERS}
        )

file(GLOB S3_SRC
        ${AWS_S3_ROOT_SRC}
        )
if (AWS_ENABLE_S3_ENDPOINT_RESOLVER)
    list(APPEND S3_SRC ${AWS_S3_ENDPOINT_RESOLVER_SRC})
endif()

add_library(${PROJECT_NAME} ${S3_HEADERS} ${S3_SRC})
aws_set_common_properties(${PROJECT_NAME})
aws_prepare_symbol_visibility_args(${PROJECT_NAME} "AWS_S3")
aws_check_headers(${PROJECT_NAME} ${AWS_S3_ROOT_HEADERS})

aws_add_sanitizers(${PROJECT_NAME})

# We are not ABI stable yet
set_target_properties(${PROJECT_NAME} PROPERTIES VERSION 1.0.0)
set_target_properties(${PROJECT_NAME} PROPERTIES SOVERSION 0unstable)

target_compile_definitions(${PROJECT_NAME} PRIVATE -DCJSON_HIDE_SYMBOLS)
if (AWS_ENABLE_S3_ENDPOINT_RESOLVER)
    target_compile_definitions(${PROJECT_NAME} PRIVATE "-DAWS_ENABLE_S3_ENDPOINT_RESOLVER")
endif()

target_include_directories(${PROJECT_NAME} PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)

aws_use_package(aws-c-auth)
aws_use_package(aws-checksums)

target_link_libraries(${PROJECT_NAME} PUBLIC ${DEP_AWS_LIBS})

aws_prepare_shared_lib_exports(${PROJECT_NAME})

install(FILES ${AWS_S3_ROOT_HEADERS} DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/aws/s3" COMPONENT Development)

if (BUILD_SHARED_LIBS)
   set (TARGET_DIR "shared")
else()
   set (TARGET_DIR "static")
endif()

install(EXPORT "${PROJECT_NAME}-targets"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}/${TARGET_DIR}/"
        NAMESPACE AWS::
        COMPONENT Development)

configure_file("cmake/${PROJECT_NAME}-config.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}-config.cmake"
        @ONLY)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}-config.cmake"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}/"
        COMPONENT Development)

include(CTest)
if (BUILD_TESTING)
    add_subdirectory(tests)
    if (NOT BYO_CRYPTO AND NOT CMAKE_CROSSCOMPILING)
        add_subdirectory(samples)
    endif()
endif()
