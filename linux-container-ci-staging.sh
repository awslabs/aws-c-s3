#!/bin/bash
# $1 - Builder version
# $2 - Image Name
# $2 - Package Name

set -e

BUILDER_VERSION=$1
shift
IMAGE_NAME=$1
shift

aws ecr get-login-password | docker login 123124136734.dkr.ecr.us-east-1.amazonaws.com -u AWS --password-stdin
export DOCKER_IMAGE=123124136734.dkr.ecr.us-east-1.amazonaws.com/${IMAGE_NAME}:${BUILDER_VERSION}
docker run --mount type=bind,source=$(pwd),target=/root/${PACKAGE_NAME} --env GITHUB_REF --env GITHUB_HEAD_REF --env ACCESS_TOKEN --env AWS_ACCESS_KEY_ID --env AWS_SECRET_ACCESS_KEY --env AWS_DEFAULT_REGION --env CXXFLAGS --env AWS_CRT_ARCH --env CTEST_PARALLEL_LEVEL $DOCKER_IMAGE --version=${BUILDER_VERSION} $@
