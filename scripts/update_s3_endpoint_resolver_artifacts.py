# This script pulls latest 'partitions.json' and 's3-endpoint-rule-set.json' from Git.
# You will need a secret in secrets manager which has the 'ruleset-url' and 'ruleset-token'.
# It uses the latest files to generate 'source/s3_endpoint_resolver/aws_s3_endpoint_rule_set.c' and
# 'source/s3_endpoint_resolver/aws_s3_endpoint_resolver_partition.c'

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
        compact_json_str: str = json.dumps(json_content, separators=(',', ':'))
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
    # Create a Secrets Manager client
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
    # Decrypts secret using the associated KMS key.
    return json.loads(get_secret_value_response['SecretString'])


def download_rule_set(secret):
    url = secret['ruleset-url']
    headers = {'Accept': 'application/vnd.github+json', 'Authorization': f"Bearer {secret['ruleset-token']}"}
    http_response = requests.get(url, headers=headers)
    if http_response.status_code != 200:
        raise Exception(f"HTTP Status code is {http_response.status_code}")

    body = json.loads(http_response.content.decode())
    http_response = requests.get(body['download_url'])
    if http_response.status_code != 200:
        raise Exception(f"HTTP Status code is {http_response.status_code}")

    return json.loads(http_response.content.decode())


def download_partition():
    url = 'https://raw.githubusercontent.com/awslabs/smithy/main/smithy-rules-engine/src/main/resources/software' \
          '/amazon/smithy/rulesengine/language/partitions.json'
    headers = {'Accept': 'application/vnd.github+json'}
    http_response = requests.get(url, headers=headers)
    if http_response.status_code != 200:
        raise Exception(f"HTTP Status code is {http_response.status_code}")

    return json.loads(http_response.content.decode())


if __name__ == '__main__':
    git_secret = get_secret_from_secrets_manager("s3/endpoint/resolver/artifacts/git", "us-east-1")

    rule_set = download_rule_set(git_secret)
    partition = download_partition()

    generate_c_file_from_json(
        rule_set,
        'source/s3_endpoint_resolver/aws_s3_endpoint_rule_set.c',
        'aws_s3_endpoint_rule_set')

    generate_c_file_from_json(
        partition,
        'source/s3_endpoint_resolver/aws_s3_endpoint_resolver_partition.c',
        'aws_s3_endpoint_resolver_partitions')
