include(AwsTestHarness)
enable_testing()

file(GLOB TEST_SRC "*.c")
file(GLOB TEST_HDRS "*.h")
file(GLOB TESTS ${TEST_HDRS} ${TEST_SRC})

# Disabling tests for now until credentials are sorted out for CI.
add_test_case(test_s3_client_create_destroy)
add_test_case(test_s3_request_create_destroy)

add_test_case(test_s3_meta_request_retry_queue_operations)
add_test_case(test_s3_meta_request_retry_queue_clean_up)
add_test_case(test_s3_meta_request_handle_error_retry)
add_test_case(test_s3_meta_request_handle_error_exceed_retries)
add_test_case(test_s3_meta_request_handle_error_fail)
add_test_case(test_s3_meta_request_fail_next_request)
add_test_case(test_s3_meta_request_fail_prepare_request)
#add_test_case(test_s3_client_sign_message_fail)
add_test_case(test_s3_meta_request_send_request_finish_fail)
add_test_case(test_s3_auto_range_put_missing_upload_id)

add_test_case(test_s3_get_object)
add_test_case(test_s3_get_object_less_than_part_size)
add_test_case(test_s3_get_object_multiple)
add_test_case(test_s3_no_signing)
add_test_case(test_s3_signing_override)
add_test_case(test_s3_put_object)
add_test_case(test_s3_put_object_less_than_part_size)
add_test_case(test_s3_meta_request_default)
add_test_case(test_s3_error_response)

set(TEST_BINARY_NAME ${PROJECT_NAME}-tests)
generate_test_driver(${TEST_BINARY_NAME})
