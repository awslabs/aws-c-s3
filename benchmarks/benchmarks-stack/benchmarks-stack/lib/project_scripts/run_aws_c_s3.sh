#!/usr/bin/env bash

if [ $1 = "SETUP" ]; then

    cd $USER_DIR
    mkdir install

    export INSTALL_PATH=$USER_DIR/install

    git clone https://github.com/awslabs/aws-lc.git
    cmake -S aws-lc -B aws-lc/build -DCMAKE_INSTALL_PREFIX=$INSTALL_PATH -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_TESTING=OFF -DENABLE_SANITIZERS=ON -DPERFORM_HEADER_CHECK=ON
    cmake --build aws-lc/build --target install

    git clone https://github.com/aws/s2n-tls.git
    cmake -S s2n-tls -B s2n-tls/build -DCMAKE_INSTALL_PREFIX=$INSTALL_PATH -DCMAKE_PREFIX_PATH=$INSTALL_PATH -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_TESTING=OFF -DENABLE_SANITIZERS=ON -DPERFORM_HEADER_CHECK=ON
    cmake --build s2n-tls/build --target install

    git clone https://github.com/awslabs/aws-c-common.git
    cmake -S aws-c-common -B aws-c-common/build -DCMAKE_INSTALL_PREFIX=$INSTALL_PATH
    cmake --build aws-c-common/build --target install

    git clone https://github.com/awslabs/aws-checksums.git
    cmake -S aws-checksums -B aws-checksums/build -DCMAKE_INSTALL_PREFIX=$INSTALL_PATH -DCMAKE_PREFIX_PATH=$INSTALL_PATH
    cmake --build aws-checksums/build --target install

    git clone https://github.com/awslabs/aws-c-sdkutils.git
    cmake -S aws-c-sdkutils -B aws-c-sdkutils/build -DCMAKE_INSTALL_PREFIX=$INSTALL_PATH -DCMAKE_PREFIX_PATH=$INSTALL_PATH
    cmake --build aws-c-sdkutils/build --target install

    git clone https://github.com/awslabs/aws-c-cal.git
    cmake -S aws-c-cal -B aws-c-cal/build -DCMAKE_INSTALL_PREFIX=$INSTALL_PATH -DCMAKE_PREFIX_PATH=$INSTALL_PATH
    cmake --build aws-c-cal/build --target install

    git clone https://github.com/awslabs/aws-c-io.git
    cmake -S aws-c-io -B aws-c-io/build -DCMAKE_INSTALL_PREFIX=$INSTALL_PATH -DCMAKE_PREFIX_PATH=$INSTALL_PATH
    cmake --build aws-c-io/build --target install

    git clone https://github.com/awslabs/aws-c-compression.git
    cmake -S aws-c-compression -B aws-c-compression/build -DCMAKE_INSTALL_PREFIX=$INSTALL_PATH -DCMAKE_PREFIX_PATH=$INSTALL_PATH
    cmake --build aws-c-compression/build --target install

    git clone https://github.com/awslabs/aws-c-http.git
    cmake -S aws-c-http -B aws-c-http/build -DCMAKE_INSTALL_PREFIX=$INSTALL_PATH -DCMAKE_PREFIX_PATH=$INSTALL_PATH
    cmake --build aws-c-http/build --target install

    git clone https://github.com/awslabs/aws-c-auth.git
    cmake -S aws-c-auth -B aws-c-auth/build -DCMAKE_INSTALL_PREFIX=$INSTALL_PATH -DCMAKE_PREFIX_PATH=$INSTALL_PATH
    cmake --build aws-c-auth/build --target install

    git clone https://github.com/awslabs/aws-c-s3.git
    cd aws-c-s3
    git checkout $BRANCH_NAME
    cd ..
    cmake -S aws-c-s3 -B aws-c-s3/build -DCMAKE_INSTALL_PREFIX=$INSTALL_PATH -DCMAKE_PREFIX_PATH=$INSTALL_PATH -DENABLE_S3_NET_TESTS=ON -DENABLE_S3_PERFORMANCE_TESTS=ON -DPERFORMANCE_TEST_NUM_TRANSFERS=100
    cmake --build aws-c-s3/build --target install

elif [ $1 = "DOWNLOAD_PERFORMANCE" ]; then

    $USER_DIR/aws-c-s3/build/tests/aws-c-s3-tests test_s3_get_performance

elif [ $1 = "UPLOAD_PERFORMANCE" ]; then

    $USER_DIR/aws-c-s3/build/tests/aws-c-s3-tests test_s3_put_performance

fi
