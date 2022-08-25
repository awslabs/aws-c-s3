project(s3-fuse C)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_INSTALL_PREFIX}/lib/cmake")

find_package(PkgConfig)
pkg_check_modules(PC_fuse QUIET fuse)
set(FUSE_VERSION ${PC_fuse_VERSION})

find_path(FUSE_INCLUDE_DIR
        NAMES fuse/fuse_lowlevel.h
        HINTS ${FUSE_ROOT} ${PC_fuse_INCLUDEDIR} ${PC_fuse_INCLUDE_DIRS}
        PATH_SUFFIXES include include/osxfuse)

if(MacOSX)
    find_library(FUSE_LIBRARY
            NAMES osxfuse
            HINTS ${FUSE_ROOT} ${PC_fuse_LIBDIR} ${PC_fuse_LIBRARY_DIRS}
            PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})
else()
    find_library(FUSE_LIBRARY
            NAMES fuse
            HINTS ${FUSE_ROOT} ${PC_fuse_LIBDIR} ${PC_fuse_LIBRARY_DIRS}
            PATH_SUFFIXES$ ${CMAKE_INSTALL_LIBDIR})

    execute_process(
            COMMAND sh -c "fusermount --version | cut -d ' ' -f 3 | cut -d '.' -f 1,2 | sed s/'\\.'//g"
            OUTPUT_VARIABLE FUSE_MOUNT_VERSION
            OUTPUT_STRIP_TRAILING_WHITESPACE
            RESULT_VARIABLE RETC)
    if(NOT ("${RETC}" STREQUAL "0") )
        set(${FUSE_MOUNT_VERSION} "" PARENT_SCOPE)
    endif()

    message(STATUS "Setting FUSE_MOUNT_VERSION: ${FUSE_MOUNT_VERSION}")
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(fuse
        REQUIRED_VARS FUSE_LIBRARY FUSE_INCLUDE_DIR
        VERSION_VAR FUSE_VERSION)

if (FUSE_FOUND AND NOT TARGET FUSE::FUSE)
    mark_as_advanced(FUSE_INCLUDE_DIR FUSE_LIBRARY)
    add_library(FUSE::FUSE UNKNOWN IMPORTED)
    set_target_properties(FUSE::FUSE PROPERTIES
            IMPORTED_LOCATION "${FUSE_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${FUSE_INCLUDE_DIR}")
endif()

if (NOT FUSE_FOUND)
    message(INFO " libfuse not found, skipping. If you'd like to build the s2-fuse sample, install libfuse into your library search path and try again")
else ()

    set(FUSE_INCLUDE_DIRS ${FUSE_INCLUDE_DIR})
    set(FUSE_LIBRARIES ${FUSE_LIBRARY})
    unset(FUSE_INCLUDE_DIR)
    unset(FUSE_LIBRARY)

    file(GLOB S3_FUSE_SRC
            "*.c"
            )

    set(S3_FUSE_PROJECT_NAME s3-fuse)

    add_executable(${S3_FUSE_PROJECT_NAME} ${S3_FUSE_SRC})
    aws_set_common_properties(${S3_FUSE_PROJECT_NAME})
    target_compile_definitions(${S3_FUSE_PROJECT_NAME} PRIVATE -D_FILE_OFFSET_BITS=64)

    target_include_directories(${S3_FUSE_PROJECT_NAME} PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:include>)

    target_include_directories(${S3_FUSE_PROJECT_NAME} PRIVATE ${FUSE_INCLUDE_DIR})
    target_link_libraries(${S3_FUSE_PROJECT_NAME} ${FUSE_LIBRARIES} aws-c-s3)

    if (BUILD_SHARED_LIBS AND NOT WIN32)
        message(INFO " s3-fuse will be built with shared libs, but you may need to set LD_LIBRARY_PATH=${CMAKE_INSTALL_PREFIX}/lib to run the application")
    endif()

    install(TARGETS ${S3_FUSE_PROJECT_NAME}
            EXPORT ${S3_FUSE_PROJECT_NAME}-targets
            COMPONENT Runtime
            RUNTIME
            DESTINATION bin
            COMPONENT Runtime)
endif()
