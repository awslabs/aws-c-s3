project(s3 C)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_INSTALL_PREFIX}/lib/cmake")

file(GLOB S3_SRC
        "*.c"
        )

set(S3_PROJECT_NAME s3)
add_executable(${S3_PROJECT_NAME} ${S3_SRC})
aws_set_common_properties(${S3_PROJECT_NAME})

set(FUNCTIONS_TO_EXPORT "malloc" "free" "realloc" "getenv" "setenv" "__errno_location"
"pthread_key_create" "pthread_key_delete" "pthread_setspecific" "pthread_getspecific"
"pthread_spin_init" "pthread_spin_destroy" "pthread_spin_lock" "pthread_spin_trylock"
"pthread_spin_unlock" "pthread_mutex_init" "pthread_mutex_destroy" "pthread_mutex_trylock"
"pthread_mutex_lock" "pthread_mutex_unlock" "pthread_cond_init" "pthread_cond_destroy"
"pthread_cond_signal" "pthread_cond_wait" "_pthread_cleanup_push" "_pthread_cleanup_pop"
"pthread_setcancelstate" "pthread_self" "pthread_yield")

foreach(FUNC ${FUNCTIONS_TO_EXPORT})
    target_link_options(${S3_PROJECT_NAME} PUBLIC "-u${FUNC}")
endforeach(FUNC)
target_include_directories(${S3_PROJECT_NAME} PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)

target_link_libraries(${S3_PROJECT_NAME} aws-c-s3)
target_link_libraries(${S3_PROJECT_NAME} ittnotify)



if (BUILD_SHARED_LIBS AND NOT WIN32)
    message(INFO " s3 will be built with shared libs, but you may need to set LD_LIBRARY_PATH=${CMAKE_INSTALL_PREFIX}/lib to run the application")
endif()

install(TARGETS ${S3_PROJECT_NAME}
        EXPORT ${S3_PROJECT_NAME}-targets
        COMPONENT Runtime
        RUNTIME
        DESTINATION bin
        COMPONENT Runtime)
