cmake_minimum_required(VERSION 3.16)
project(UploadTest)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 查找依赖包
find_package(PkgConfig REQUIRED)
find_package(AWSSDK REQUIRED COMPONENTS s3)
find_package(CURL REQUIRED)
find_package(Threads REQUIRED)

# 查找jsoncpp
pkg_check_modules(JSONCPP jsoncpp)
if(NOT JSONCPP_FOUND)
    find_package(jsoncpp REQUIRED)
    set(JSONCPP_LIBRARIES jsoncpp_lib)
endif()

# 创建同步版本可执行文件
add_executable(upload_sync upload_sync.cpp)

# 创建异步版本可执行文件
add_executable(upload_async upload_async.cpp)

# 链接库 - 同步版本
target_link_libraries(upload_sync 
    ${AWSSDK_LINK_LIBRARIES}
    ${CURL_LIBRARIES}
    ${JSONCPP_LIBRARIES}
)

# 链接库 - 异步版本
target_link_libraries(upload_async 
    ${AWSSDK_LINK_LIBRARIES}
    ${CURL_LIBRARIES}
    ${JSONCPP_LIBRARIES}
    Threads::Threads
)

# 包含头文件目录 - 同步版本
target_include_directories(upload_sync PRIVATE
    ${CURL_INCLUDE_DIRS}
    ${JSONCPP_INCLUDE_DIRS}
)

# 包含头文件目录 - 异步版本
target_include_directories(upload_async PRIVATE
    ${CURL_INCLUDE_DIRS}
    ${JSONCPP_INCLUDE_DIRS}
)

# 编译选项 - 同步版本
target_compile_options(upload_sync PRIVATE ${JSONCPP_CFLAGS_OTHER})

# 编译选项 - 异步版本
target_compile_options(upload_async PRIVATE ${JSONCPP_CFLAGS_OTHER})