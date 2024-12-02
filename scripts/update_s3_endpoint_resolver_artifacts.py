# This script pulls latest 'partitions.json' and 's3-endpoint-rule-set.json' from Git.
# You will need a secret in secrets manager which has the 'ruleset-url' and 'ruleset-token'.
# It uses the latest files to generate 'source/s3_endpoint_resolver/aws_s3_endpoint_rule_set.c' and
# 'source/s3_endpoint_resolver/aws_s3_endpoint_resolver_partition.c'

import argparse
import json
import boto3
import requests


def escape_char(c):
    escape_dict = {
        '\\': '\\\\',
        '\'': '\\\'',
        '\0': '\\0',
        '\a': '\\a',
        '\b': '\\b',
        '\f': '\\f',
        '\n': '\\n',
        '\r': '\\r',
        '\t': '\\t',
        '\v': '\\v'
    }

    return escape_dict.get(c, c)


def get_header():
    return """\
/**
 * Copyright Amazon.com, Inc. or its affiliates.
 * All Rights Reserved. SPDX-License-Identifier: Apache-2.0.
 */
#include "aws/s3/private/s3_endpoint_resolver.h"
#include <aws/s3/s3_endpoint_resolver.h>

/**
 * This file is generated using scripts/update_s3_endpoint_resolver_artifacts.py.
 * Do not modify directly. */
/* clang-format off */

"""


def generate_c_file_from_json(json_content, c_file_name, c_struct_name):
    num_chars_per_line = 20

    try:
        # Compact the json
        compact_json_str = json.dumps(json_content, separators=(',', ':'))
        compact_c = []
        for i in range(0, len(compact_json_str), num_chars_per_line):
            compact_c.append(
                ', '.join("'{}'".format(escape_char(char)) for char in compact_json_str[i:i + num_chars_per_line]))

        # Write json to a C file
        with open(c_file_name, 'w') as f:
            f.write(get_header())
            f.write(f"static const char s_generated_array[] = {{\n\t")
            f.write(",\n\t".join(compact_c))
            f.write("};\n\n")

            f.write(f"const struct aws_byte_cursor {c_struct_name} = {{\n\t")
            f.write(f".len = {len(compact_json_str)},\n\t")
            f.write(f".ptr = (uint8_t *) s_generated_array\n}};\n")

        print(f"{c_file_name} has been created successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")


def get_secret_from_secrets_manager(secret_name, region_name):
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as e:
        raise e

    return json.loads(get_secret_value_response['SecretString'])


def download_from_git(url, token=None):
    headers = {'Accept': 'application/vnd.github+json'}
    if token is not None:
        headers['Authorization'] = f"Bearer {token}"
    http_response = requests.get(url, headers=headers)
    if http_response.status_code != 200:
        raise Exception(f"HTTP Status code is {http_response.status_code}")

    return json.loads(http_response.content.decode())


if __name__ == '__main__':
    argument_parser = argparse.ArgumentParser(description="Endpoint Ruleset Updater")
    argument_parser.add_argument("--ruleset", metavar="<Path to ruleset>",
                                required=False, help="Path to endpoint ruleset json file")
    argument_parser.add_argument("--partitions", metavar="<Path to partitions>",
                                required=False, help="Path to partitions json file")
    parsed_args = argument_parser.parse_args()

    git_secret = get_secret_from_secrets_manager("s3/endpoint/resolver/artifacts/git", "us-east-1")

    if (parsed_args.ruleset):
        with open(parsed_args.ruleset) as f:
           rule_set = json.load(f)    
    else:
        rule_set = download_from_git(git_secret['ruleset-url'], git_secret['ruleset-token'])

    if (parsed_args.partitions):    
        with open(parsed_args.partitions) as f:
           partition = json.load(f) 
    else:
        partition = download_from_git('https://raw.githubusercontent.com/aws/aws-sdk-cpp/main/tools/code-generation/partitions/partitions.json')

    generate_c_file_from_json(
        rule_set,
        'source/s3_endpoint_resolver/aws_s3_endpoint_rule_set.c',
        'aws_s3_endpoint_rule_set')

    generate_c_file_from_json(
        partition,
        'source/s3_endpoint_resolver/aws_s3_endpoint_resolver_partition.c',
        'aws_s3_endpoint_resolver_partitions')
