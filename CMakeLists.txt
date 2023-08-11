
cmake_minimum_required(VERSION 3.1)
project(aws-c-s3 C)

if (POLICY CMP0069)
    cmake_policy(SET CMP0069 NEW) # Enable LTO/IPO if available in the compiler, see AwsCFlags
endif()

option(ASSERT_LOCK_HELD "Enable ASSERT_SYNCED_DATA_LOCK_HELD for checking thread issue" OFF)
option(ENABLE_MOCK_SERVER_TESTS "Whether to run the integration tests that rely on pre-configured mock server" OFF)
option(ENABLE_MRAP_TESTS "Whether to run the integration tests that rely on pre-configured multi-region access point" OFF)
option(AWS_ENABLE_S3_ENDPOINT_RESOLVER "Whether to include the s3 endpoint resolver and related config files" OFF)

if (ASSERT_LOCK_HELD)
    add_definitions(-DASSERT_LOCK_HELD)
endif()

if (DEFINED CMAKE_PREFIX_PATH)
    file(TO_CMAKE_PATH "${CMAKE_PREFIX_PATH}" CMAKE_PREFIX_PATH)
endif()

if (DEFINED CMAKE_INSTALL_PREFIX)
    file(TO_CMAKE_PATH "${CMAKE_INSTALL_PREFIX}" CMAKE_INSTALL_PREFIX)
endif()


if (UNIX AND NOT APPLE)
    include(GNUInstallDirs)
elseif(NOT DEFINED CMAKE_INSTALL_LIBDIR)
    set(CMAKE_INSTALL_LIBDIR "lib")
endif()

# This is required in order to append /lib/cmake to each element in CMAKE_PREFIX_PATH
set(AWS_MODULE_DIR "/${CMAKE_INSTALL_LIBDIR}/cmake")
string(REPLACE ";" "${AWS_MODULE_DIR};" AWS_MODULE_PATH "${CMAKE_PREFIX_PATH}${AWS_MODULE_DIR}")
# Append that generated list to the module search path
list(APPEND CMAKE_MODULE_PATH ${AWS_MODULE_PATH})

include(AwsCFlags)
include(AwsCheckHeaders)
include(AwsSharedLibSetup)
include(AwsSanitizers)
include(AwsFindPackage)

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
    set(PLATFORM_LIBS "")
elseif (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(PLATFORM_LIBS "")
elseif (APPLE)
    set(PLATFORM_LIBS "")
elseif (CMAKE_SYSTEM_NAME STREQUAL "FreeBSD" OR CMAKE_SYSTEM_NAME STREQUAL "NetBSD")
    set(PLATFORM_LIBS "")
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

install(FILES ${AWS_S3_ROOT_HEADERS} DESTINATION "include/aws/s3" COMPONENT Development)

if (BUILD_SHARED_LIBS)
   set (TARGET_DIR "shared")
else()
   set (TARGET_DIR "static")
endif()

install(EXPORT "${PROJECT_NAME}-targets"
        DESTINATION "${LIBRARY_DIRECTORY}/${PROJECT_NAME}/cmake/${TARGET_DIR}/"
        NAMESPACE AWS::
        COMPONENT Development)

configure_file("cmake/${PROJECT_NAME}-config.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}-config.cmake"
        @ONLY)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}-config.cmake"
        DESTINATION "${LIBRARY_DIRECTORY}/${PROJECT_NAME}/cmake/"
        COMPONENT Development)

include(CTest)
if (BUILD_TESTING)
    add_subdirectory(tests)
    if (NOT BYO_CRYPTO AND NOT CMAKE_CROSSCOMPILING)
        add_subdirectory(samples)
    endif()
endif()
