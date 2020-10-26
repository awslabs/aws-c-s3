include(AwsTestHarness)
enable_testing()

file(GLOB TEST_SRC "*.c")
file(GLOB TEST_HDRS "*.h")
file(GLOB TESTS ${TEST_HDRS} ${TEST_SRC})

# Disabling tests for now until credentials are sorted out for CI.
add_test_case(test_s3_client_create_destroy)
add_test_case(test_s3_get_object)
add_test_case(test_s3_get_object_multiple)
add_test_case(test_s3_get_object)
add_test_case(test_s3_put_object)

set(TEST_BINARY_NAME ${PROJECT_NAME}-tests)
generate_test_driver(${TEST_BINARY_NAME})
