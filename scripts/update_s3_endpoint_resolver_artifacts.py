# This script pulls latest 'partitions.json' and 's3-endpoint-rule-set.json' from 'aws-c-s3-endpoint-artifacts' S3 bucket.
# It uses the latest files to generate 'source/s3_endpoint_resolver/aws_s3_endpoint_rule_set.c' and
# 'source/s3_endpoint_resolver/aws_s3_endpoint_resolver_partition.c'

import json
import boto3

def escape_char(c):
    if c == '\\':
        return '\\\\'
    elif c == '\'':
        return '\\\''
    elif c == '\a':
        return '\\a'
    elif c == '\b':
        return '\\b'
    elif c == '\f':
        return '\\f'
    elif c == '\n':
        return '\\n'
    elif c == '\r':
        return '\\r'
    elif c == '\t':
        return '\\t'
    elif c == '\v':
        return '\\v'
    else:
        return c

def get_header():
    return """\
/**
* Copyright Amazon.com, Inc. or its affiliates.
* All Rights Reserved. SPDX-License-Identifier: Apache-2.0.
*/

#include <aws/s3/s3_endpoint_resolver.h>

/**
 * This file is generated using scripts/update_s3_endpoint_resolver_artifacts.py.
 * Do not modify directly. */
/* clang-format off */

"""

def generate_c_file_from_json(s3, bucket_name, s3_file_name, c_file_name, c_struct_name):
    num_chars_per_line = 20

    try:
        # Retrieve the JSON file from S3
        response = s3.get_object(Bucket=bucket_name, Key=s3_file_name)
        json_content_str = response['Body'].read().decode()
        json_content = json.loads(json_content_str)
        # Compact the json
        compact_json_str: str = json.dumps(json_content, separators=(',', ':'))

        # Write json to a C file
        with open(c_file_name, 'w') as f:
            f.write(get_header())
            f.write(f"const char {c_struct_name}[] = {{\n\t")
            for i in range(0, len(compact_json_str), num_chars_per_line):
                f.write(', '.join("'{}'".format(escape_char(char)) for char in compact_json_str[i:i + num_chars_per_line]))
                if i + 15 < len(compact_json_str):
                    f.write(",\n\t")
            f.write(", '\\0'};\n")

        print(f"{c_file_name} has been created successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == '__main__':
    session = boto3.session.Session()
    s3 = session.client('s3', region_name='us-east-1')
    bucket_name = 'aws-c-s3-models'

    generate_c_file_from_json(
        s3,
        bucket_name,
        's3-endpoint-rule-set.json',
        'source/s3_endpoint_resolver/aws_s3_endpoint_rule_set.c',
        'aws_s3_endpoint_rule_set')

    generate_c_file_from_json(
        s3,
        bucket_name,
        'partitions.json',
        'source/s3_endpoint_resolver/aws_s3_endpoint_resolver_partition.c',
        'aws_s3_endpoint_resolver_partitions')
