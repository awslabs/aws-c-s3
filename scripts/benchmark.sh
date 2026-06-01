#!/usr/bin/env bash
set -euo pipefail

# Benchmark script for aws-c-s3 s3-cp sample.
# Builds the full CRT dependency chain from source, then runs an upload/download
# and reports throughput.
#
# Usage:
#   ./scripts/benchmark.sh [--branch <branch>] [--region <region>] [--bucket <bucket>] [--key <key>] [--file-size-mb <size>]
#
# Prerequisites:
#   - cmake, gcc/clang, git
#   - Valid AWS credentials in the environment

BRANCH="${BRANCH:-add-gpu-instance-platform-info}"
REGION="${REGION:-<YOUR_REGION>}"
BUCKET="${BUCKET:-<YOUR_BUCKET>}"
KEY="${KEY:-benchmark-test-object}"
FILE_SIZE_MB="${FILE_SIZE_MB:-1024}"
INSTALL_DIR=""
BUILD_DIR=""
WORK_DIR=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --branch) BRANCH="$2"; shift 2;;
        --region) REGION="$2"; shift 2;;
        --bucket) BUCKET="$2"; shift 2;;
        --key) KEY="$2"; shift 2;;
        --file-size-mb) FILE_SIZE_MB="$2"; shift 2;;
        *) echo "Unknown option: $1"; exit 1;;
    esac
done

WORK_DIR="$(mktemp -d)"
BUILD_DIR="${WORK_DIR}/build"
INSTALL_DIR="${WORK_DIR}/install"

echo "=== aws-c-s3 benchmark ==="
echo "Branch:    ${BRANCH}"
echo "Region:    ${REGION}"
echo "Bucket:    ${BUCKET}"
echo "File size: ${FILE_SIZE_MB} MB"
echo "Work dir:  ${WORK_DIR}"
echo ""

cleanup() {
    echo "Cleaning up ${WORK_DIR}..."
    rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

# Clone and build a single dependency
build_dep() {
    local name="$1"
    local repo="https://github.com/awslabs/${name}.git"
    local src="${WORK_DIR}/${name}"

    echo "--- Building ${name} ---"
    git clone --depth 1 "${repo}" "${src}" 2>/dev/null
    cmake -S "${src}" -B "${BUILD_DIR}/${name}" \
        -DCMAKE_INSTALL_PREFIX="${INSTALL_DIR}" \
        -DCMAKE_PREFIX_PATH="${INSTALL_DIR}" \
        -DBUILD_TESTING=OFF \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_SHARED_LIBS=OFF \
        > /dev/null 2>&1
    cmake --build "${BUILD_DIR}/${name}" --parallel > /dev/null 2>&1
    cmake --install "${BUILD_DIR}/${name}" > /dev/null 2>&1
}

# Build dependencies in order
build_dep "aws-c-common"
build_dep "aws-c-cal"
build_dep "aws-c-io"
build_dep "s2n-tls"
build_dep "aws-c-compression"
build_dep "aws-c-http"
build_dep "aws-c-sdkutils"
build_dep "aws-checksums"
build_dep "aws-c-auth"

# Build aws-c-s3 from the specified branch
echo "--- Building aws-c-s3 (branch: ${BRANCH}) ---"
S3_SRC="${WORK_DIR}/aws-c-s3"
git clone --branch "${BRANCH}" --depth 1 "https://github.com/awslabs/aws-c-s3.git" "${S3_SRC}" 2>/dev/null
cmake -S "${S3_SRC}" -B "${BUILD_DIR}/aws-c-s3" \
    -DCMAKE_INSTALL_PREFIX="${INSTALL_DIR}" \
    -DCMAKE_PREFIX_PATH="${INSTALL_DIR}" \
    -DBUILD_TESTING=OFF \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=OFF \
    > /dev/null 2>&1
cmake --build "${BUILD_DIR}/aws-c-s3" --parallel > /dev/null 2>&1
cmake --install "${BUILD_DIR}/aws-c-s3" > /dev/null 2>&1

S3_BIN="${BUILD_DIR}/aws-c-s3/samples/s3/s3"

if [[ ! -x "${S3_BIN}" ]]; then
    echo "ERROR: s3 binary not found at ${S3_BIN}"
    exit 1
fi

echo ""
echo "=== Build complete ==="
echo ""

# Generate test file
TEST_FILE="${WORK_DIR}/test_upload_file"
echo "Generating ${FILE_SIZE_MB} MB test file..."
dd if=/dev/urandom of="${TEST_FILE}" bs=1M count="${FILE_SIZE_MB}" status=none

# Upload benchmark
echo ""
echo "=== Upload benchmark (${FILE_SIZE_MB} MB) ==="
"${S3_BIN}" -r "${REGION}" cp "${TEST_FILE}" "s3://${BUCKET}/${KEY}"

# Download benchmark
DOWNLOAD_FILE="${WORK_DIR}/test_download_file"
echo ""
echo "=== Download benchmark ==="
"${S3_BIN}" -r "${REGION}" cp "s3://${BUCKET}/${KEY}" "${DOWNLOAD_FILE}"

echo ""
echo "=== Done ==="
