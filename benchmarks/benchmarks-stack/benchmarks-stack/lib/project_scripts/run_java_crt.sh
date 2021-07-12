#!/usr/bin/env bash

cd $USER_DIR

if [ $1 = "SETUP" ]; then
    sudo yum install java-1.8.0-devel -y

    sudo alternatives --set java /usr/lib/jvm/jre-1.8.0-openjdk.x86_64/bin/java
    sudo alternatives --set javac /usr/lib/jvm/java-1.8.0-openjdk.x86_64/bin/javac
    sudo yum install maven -y

    git clone https://github.com/awslabs/aws-crt-java.git --recursive
    cd aws-crt-java

    git checkout $BRANCH_NAME
    git submodule init
    git submodule update
    mvn install -DskipTests

elif [ $1 = "DOWNLOAD_PERFORMANCE" ]; then
    cd aws-crt-java

    mvn test -DforkCount=0 -Dtest="S3ClientTest#benchmarkS3Get" -Daws.crt.s3.benchmark=1 \
        -Daws.crt.s3.benchmark.region=$REGION \
        -Daws.crt.s3.benchmark.gbps=$THROUGHPUT_GBPS \
        -Daws.crt.s3.benchmark.transfers=1600 \
        -Daws.crt.s3.benchmark.concurrent=1600 \
        -Daws.crt.s3.benchmark.object=$TEST_OBJECT_NAME \
        -Daws.crt.s3.benchmark.bucket=$S3_BUCKET_NAME \
        -Daws.crt.s3.benchmark.threads=18 \
        -Daws.crt.s3.benchmark.warmup=30 \
        -Daws.crt.s3.benchmark.tls=true

elif [ $1 = "UPLOAD_PERFORMANCE" ]; then
    cd aws-crt-java

    mvn test -DforkCount=0 -Dtest="S3ClientTest#benchmarkS3Put" -Daws.crt.s3.benchmark=1 \
        -Daws.crt.s3.benchmark.region=$REGION \
        -Daws.crt.s3.benchmark.gbps=$THROUGHPUT_GBPS \
        -Daws.crt.s3.benchmark.bucket=$S3_BUCKET_NAME \
        -Daws.crt.s3.benchmark.transfers=1600 \
        -Daws.crt.s3.benchmark.concurrent=1600 \
        -Daws.crt.s3.benchmark.threads=18 \
        -Daws.crt.s3.benchmark.warmup=30 \
        -Daws.crt.s3.benchmark.tls=true
fi
