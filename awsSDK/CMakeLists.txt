cmake_minimum_required(VERSION 3.16)
project(AwsS3Test)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# set(CMAKE_PREFIX_PATH "/home/sjh/eloqstore/aws-sdk-install" ${CMAKE_PREFIX_PATH})
# 使用系统安装的AWS SDK
find_package(AWSSDK REQUIRED COMPONENTS s3 transfer)

# AWS测试可执行文件
add_executable(main main.cpp)
target_link_libraries(main
    ${AWSSDK_LINK_LIBRARIES}
    pthread curl)
