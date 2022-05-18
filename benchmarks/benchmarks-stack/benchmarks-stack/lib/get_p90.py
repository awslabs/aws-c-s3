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
    localmax = max(bytes_result)
    return p90, localmax


def publish_metric(metric_name, project_name, branch_name, instance_name, value):
    os.system("aws cloudwatch put-metric-data \
            --no-cli-pager \
            --namespace S3Benchmark \
            --metric-name {} \
            --unit Gigabits/Second \
            --dimensions Project={},Branch={},InstanceType={} \
            --value {}".format(metric_name, project_name, branch_name, instance_name, value))


file_path = sys.argv[1]
project_name = sys.argv[2]
branch_name = sys.argv[3]
instance_name = sys.argv[4]


p90, localmax = get_p90_max(file_path)

metric_prefix = "BytesIn"
if "BytesOut" in file_path:
    metric_prefix = "BytesOut"
publish_metric("{}P90".format(metric_prefix), project_name,
               branch_name, instance_name, p90)
publish_metric("{}Max".format(metric_prefix), project_name,
               branch_name, instance_name, localmax)
