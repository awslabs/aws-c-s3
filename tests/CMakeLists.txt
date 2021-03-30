include(AwsTestHarness)
enable_testing()

option(ENABLE_S3_NET_TESTS "Run S3 tests requiring internet connection and credentials" OFF)
option(ENABLE_S3_PERFORMANCE_TESTS "Run S3 performance tests" OFF)
option(PERFORMANCE_TEST_LOGGING "Whether performance tests should log output" OFF)
option(PERFORMANCE_TEST_VALIDATE_PUTS, "Whether put operations should be validated during performance tests" OFF)

set(S3_TEST_BUCKET "aws-crt-canary-bucket-eu-west-1" CACHE STRING "Name of the bucket to use for tests" )
set(S3_TEST_REGION "eu-west-1" CACHE STRING "Name of the region to use for tests")
set(PERFORMANCE_TEST_THROUGHPUT_TARGET_GBPS "100.0" CACHE STRING "Throughput target for performance tests")
set(PERFORMANCE_TEST_NUM_TRANSFERS "1600" CACHE STRING "Number of transfers to do during performance tests")
set(PERFORMANCE_TEST_UPLOAD_OBJECT_SIZE_MB "5120" CACHE STRING "Size of the object in megabytes to upload during performance tests")

add_definitions(-DS3_TEST_BUCKET="${S3_TEST_BUCKET}")
add_definitions(-DS3_TEST_REGION="${S3_TEST_REGION}")
add_definitions(-DPERFORMANCE_TEST_THROUGHPUT_TARGET_GBPS=${PERFORMANCE_TEST_THROUGHPUT_TARGET_GBPS})
add_definitions(-DPERFORMANCE_TEST_NUM_TRANSFERS=${PERFORMANCE_TEST_NUM_TRANSFERS})
add_definitions(-DPERFORMANCE_TEST_UPLOAD_OBJECT_SIZE_MB=${PERFORMANCE_TEST_UPLOAD_OBJECT_SIZE_MB})

if(PERFORMANCE_TEST_VALIDATE_PUTS)
    add_definitions(-DPERFORMANCE_TEST_VALIDATE_PUTS)
endif()

if(PERFORMANCE_TEST_LOGGING)
    add_definitions(-DPERFORMANCE_TEST_LOGGING_ENABLED)
endif()

macro(add_s3_net_test_case name)
    if (ENABLE_S3_NET_TESTS)
        list(APPEND TEST_CASES "${name}")
        list(APPEND AWS_TEST_CASES "${name}")
        set_property(GLOBAL PROPERTY AWS_TEST_CASES ${AWS_TEST_CASES})
    endif()
endmacro()

file(GLOB TEST_SRC "*.c")
file(GLOB TEST_HDRS "*.h")
file(GLOB TESTS ${TEST_HDRS} ${TEST_SRC})

if(ENABLE_S3_PERFORMANCE_TESTS)
    add_s3_net_test_case(test_s3_get_performance)
    add_s3_net_test_case(test_s3_put_performance)
endif()

set(TEST_BINARY_NAME ${PROJECT_NAME}-tests)
generate_test_driver(${TEST_BINARY_NAME})
