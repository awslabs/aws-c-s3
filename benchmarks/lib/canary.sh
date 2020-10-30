#!/bin/bash

set -ex

if [ ! -x /home/ec2-user/install/bin/aws-crt-cpp-canary ]; then
    cd /tmp
    yum update -y
    yum install -y cmake3 git gcc72 gcc72-c++ htop
    git clone https://github.com/awslabs/aws-crt-cpp.git
    cd aws-crt-cpp
    git checkout s3_canary_vertical
    git submodule update --init
    cd aws-common-runtime/s2n/libcrypto-build
    curl -LO https://www.openssl.org/source/openssl-1.1.1-latest.tar.gz
    tar -xzvf openssl-1.1.1-latest.tar.gz
    cd `tar ztf openssl-1.1.1-latest.tar.gz | head -n1 | cut -f1 -d/`
    ./config -fPIC no-shared no-md2 no-rc5 no-rfc3779 no-sctp no-ssl-trace no-zlib no-hw no-mdc2 no-seed no-idea enable-ec_nistp_64_gcc_128 no-camellia no-bf no-ripemd no-dsa no-ssl2 no-ssl3 no-capieng -DSSL_FORBID_ENULL -DOPENSSL_NO_DTLS1 -DOPENSSL_NO_HEARTBEATS --prefix=/home/ec2-user/install
    make -j
    make install_sw
    mkdir -p /tmp/aws-crt-cpp/build
    cd /tmp/aws-crt-cpp/build
    cmake3 .. -DCMAKE_BUILD_TYPE=Release -DBUILD_DEPS=true -DCMAKE_INSTALL_PREFIX=/home/ec2-user/install -DCMAKE_PREFIX_PATH=/home/ec2-user/install
    cmake3 --build . --target install -- -j
fi


if [ -n "$1" ]; then
    /home/ec2-user/install/bin/aws-crt-cpp-canary -g $1
fi
