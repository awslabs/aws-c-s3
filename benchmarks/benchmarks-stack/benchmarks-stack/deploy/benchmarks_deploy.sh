# initialize the env
UserName=${1:-ec2-user}
ProjectName=${2:-aws-c-s3}
StackName=${3:-aws-c-s3-c5n}
InstanceConfigName=${4:-c5n.18xlarge}
ThroughputGbps=${5:-100}

# ./node_modules/aws-cdk/bin/cdk deploy -v --require-approval never -c UserName=$UserName -c ProjectName=$ProjectName -c StackName=$StackName -c InstanceConfigName=$InstanceConfigName -c ThroughputGbps=$ThroughputGbps
./node_modules/aws-cdk/bin/cdk deploy -v --require-approval never
