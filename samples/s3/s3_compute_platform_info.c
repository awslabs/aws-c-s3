/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/command_line_parser.h>
#include <aws/s3/s3_platform_info.h>

#include "app_ctx.h"

struct s3_compute_platform_ctx {
    struct app_ctx *app_ctx;
    struct aws_byte_cursor instance_type;
};

static void s_usage(int exit_code) {
    FILE *output = exit_code == 0 ? stdout : stderr;
    fprintf(output, "usage: s3 platform-info [options]\n");
    fprintf(
        output,
        "  -instance-type, (optional) Instance type to look up configuration for, if not set it will be the current "
        "executing environment. \n");
    fprintf(output, "  -h, --help\n");
    fprintf(output, "            Display this message and quit.\n");
    exit(exit_code);
}

static struct aws_cli_option s_long_options[] = {
    {"instance-type", AWS_CLI_OPTIONS_REQUIRED_ARGUMENT, NULL, 'i'},
    /* Per getopt(3) the last element of the array has to be filled with all zeros */
    {NULL, AWS_CLI_OPTIONS_NO_ARGUMENT, NULL, 0},
};

static void s_parse_options(int argc, char **argv, struct s3_compute_platform_ctx *ctx) {
    int option_index = 0;

    int opt_val = 0;
    do {
        opt_val = aws_cli_getopt_long(argc, argv, "i:", s_long_options, &option_index);
        /* START_OF_TEXT means our positional argument */
        if (opt_val == 'i') {
            ctx->instance_type = aws_byte_cursor_from_c_str(aws_cli_optarg);
        }
    } while (opt_val != -1);
}

int s3_compute_platform_info_main(int argc, char *argv[], const char *command_name, void *user_data) {
    (void)command_name;

    struct app_ctx *app_ctx = user_data;

    if (app_ctx->help_requested) {
        s_usage(0);
    }

    struct s3_compute_platform_ctx compute_platform_app_ctx = {
        .app_ctx = app_ctx,
    };
    app_ctx->sub_command_data = &compute_platform_app_ctx;

    s_parse_options(argc, argv, &compute_platform_app_ctx);

    struct aws_s3_compute_platform_info_loader *loader = aws_s3_compute_platform_info_loader_new(app_ctx->allocator);
    if (!loader) {
        fprintf(stderr, "failed to load configuration info with error %s", aws_error_debug_str(aws_last_error()));
        exit(-1);
    }

    const struct aws_s3_compute_platform_info *platform_info = aws_s3_current_compute_platform_info();

    if (compute_platform_app_ctx.instance_type.len) {
        platform_info =
            aws_s3_get_compute_platform_info_for_instance_type(loader, compute_platform_app_ctx.instance_type);
        if (!platform_info) {
            fprintf(
                stderr,
                "unknown instance type \"" PRInSTR "\"",
                AWS_BYTE_CURSOR_PRI(compute_platform_app_ctx.instance_type));
            exit(-1);
        }
    }
    fprintf(stdout, "{\n");
    fprintf(stdout, "\t'instance_type': '" PRInSTR "',\n", AWS_BYTE_CURSOR_PRI(platform_info->instance_type));
    fprintf(stdout, "\t'max_throughput_gbps': %d,\n", (int)platform_info->max_throughput_gbps);
    fprintf(
        stdout,
        "\t'has_recommended_configuration': %s,\n",
        platform_info->has_recommended_configuration ? "true" : "false");

    fprintf(stdout, "\t'cpu_groups': [\n");

    for (size_t i = 0; i < platform_info->cpu_group_info_array_length; ++i) {
        fprintf(stdout, "\t{\n");
        fprintf(stdout, "\t\t'cpu_group_index': %d,\n", (int)platform_info->cpu_group_info_array[i].cpu_group);
        fprintf(stdout, "\t\t'cpus_in_group': %d,\n", (int)platform_info->cpu_group_info_array[i].cpus_in_group);
        fprintf(stdout, "\t\t'usable_network_devices': [\n");

        for (size_t j = 0; j < platform_info->cpu_group_info_array[i].nic_name_array_length; j++) {
            fprintf(
                stdout,
                "\t\t\t'" PRInSTR "'",
                AWS_BYTE_CURSOR_PRI(platform_info->cpu_group_info_array[i].nic_name_array[j]));
            if (j < platform_info->cpu_group_info_array[i].nic_name_array_length - 1) {
                fprintf(stdout, ",");
            }
            fprintf(stdout, "\n");
        }
        fprintf(stdout, "\t\t]\n");
        fprintf(stdout, "\t}");
        if (i < platform_info->cpu_group_info_array_length - 1) {
            fprintf(stdout, ",");
        }
        fprintf(stdout, "\n");
    }
    fprintf(stdout, "\t]\n");
    fprintf(stdout, "}\n");

    return 0;
}
