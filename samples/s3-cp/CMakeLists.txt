project(s3-cp C)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_INSTALL_PREFIX}/lib/cmake")

file(GLOB S3_CP_SRC
        "*.c"
        )

set(S3_CP_PROJECT_NAME s3-cp)
add_executable(${S3_CP_PROJECT_NAME} ${S3_CP_SRC})
aws_set_common_properties(${S3_CP_PROJECT_NAME})


target_include_directories(${S3_CP_PROJECT_NAME} PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)

target_link_libraries(${S3_CP_PROJECT_NAME} aws-c-s3)

if (BUILD_SHARED_LIBS AND NOT WIN32)
    message(INFO " s3-cp will be built with shared libs, but you may need to set LD_LIBRARY_PATH=${CMAKE_INSTALL_PREFIX}/lib to run the application")
endif()

install(TARGETS ${S3_CP_PROJECT_NAME}
        EXPORT ${S3_CP_PROJECT_NAME}-targets
        COMPONENT Runtime
        RUNTIME
        DESTINATION bin
        COMPONENT Runtime)
