/**
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0.
*/
#include <fuse.h>

#include <aws/common/command_line_parser.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/auth/credentials.h>

#include "app_ctx.h"
 /*
static struct fuse_operations fuse_vtable = {
    .
};*/

int main(int argc, char *argv[]) {
    (void)argc;
    (void)argv;

    struct aws_allocator *allocator = aws_default_allocator();
    aws_s3_library_init(allocator);

    struct app_ctx app_ctx;
    AWS_ZERO_STRUCT(app_ctx);
    app_ctx.allocator = allocator;
    app_ctx.c_var = (struct aws_condition_variable)AWS_CONDITION_VARIABLE_INIT;
    aws_mutex_init(&app_ctx.mutex);

    /* event loop */
    struct aws_event_loop_group *event_loop_group = aws_event_loop_group_new_default(allocator, 0, NULL);

    /* resolver */
    struct aws_host_resolver_default_options resolver_options = {
        .el_group = event_loop_group,
        .max_entries = 8,
    };
    struct aws_host_resolver *resolver = aws_host_resolver_new_default(allocator, &resolver_options);

    /* client bootstrap */
    struct aws_client_bootstrap_options bootstrap_options = {
        .event_loop_group = event_loop_group,
        .host_resolver = resolver,
    };
    app_ctx.client_bootstrap = aws_client_bootstrap_new(allocator, &bootstrap_options);
    if (app_ctx.client_bootstrap == NULL) {
        printf("ERROR initializing client bootstrap\n");
        return -1;
    }

    /* credentials */
    struct aws_credentials_provider_chain_default_options credentials_provider_options;
    AWS_ZERO_STRUCT(credentials_provider_options);
    credentials_provider_options.bootstrap = app_ctx.client_bootstrap;
    app_ctx.credentials_provider = aws_credentials_provider_new_chain_default(allocator, &credentials_provider_options);

    /*fuse init here */

    /* release resources */
    aws_s3_client_release(app_ctx.client);
    aws_credentials_provider_release(app_ctx.credentials_provider);
    aws_client_bootstrap_release(app_ctx.client_bootstrap);
    aws_host_resolver_release(resolver);
    aws_event_loop_group_release(event_loop_group);
    aws_mutex_clean_up(&app_ctx.mutex);
    aws_s3_library_clean_up();

    return 0;
}
