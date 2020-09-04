include(AwsTestHarness)
enable_testing()

file(GLOB TEST_SRC "*.c")
file(GLOB TEST_HDRS "*.h")
file(GLOB TESTS ${TEST_HDRS} ${TEST_SRC})

# Disabling tests for now until credentials are sorted out for CI.

set(TEST_BINARY_NAME ${PROJECT_NAME}-tests)
#generate_test_driver(${TEST_BINARY_NAME})
