import string
import sys
import numpy as np
import os


def get_p90_max(filepath: string):
    bytes_result = []
    with open(filepath) as f:
        for i in f.readlines():
            bytes_result.append(float(i)*8/1000/1000/1000)
    npy_array = np.array(bytes_result)
    p90 = np.percentile(npy_array, 90)
    max = max(bytes_result)
    return p90, max


def publish_metric(metric_name, project_name, branch_name, instance_name, value):
    os.system("aws cloudwatch put-metric-data \
            --no-cli-pager \
            --namespace S3Benchmark \
            --metric-name {} \
            --unit Gigabits/Second \
            --dimensions Project={},Branch={},InstanceType={} \
            --value {}".format(metric_name, project_name, branch_name, instance_name, value))


byte_in_file_path = sys.argv[1]
byte_out_file_path = sys.argv[2]
project_name = sys.argv[3]
branch_name = sys.argv[4]
instance_name = sys.argv[5]

in_p90, in_max = get_p90_max(byte_in_file_path)
publish_metric("BytesInP90", project_name, branch_name, instance_name, in_p90)
publish_metric("BytesInMax", project_name, branch_name, instance_name, in_max)

out_p90, out_max = get_p90_max(byte_out_file_path)
publish_metric("BytesOutP90", project_name,
               branch_name, instance_name, out_p90)
publish_metric("BytesOutMax", project_name,
               branch_name, instance_name, out_max)
