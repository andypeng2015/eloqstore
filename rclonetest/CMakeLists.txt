cmake_minimum_required(VERSION 3.20)
project(RclonePerfTest)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 查找必要的依赖包
find_package(Threads REQUIRED)
find_package(Boost REQUIRED COMPONENTS context)

if(Boost_FOUND)
    message(STATUS "Boost found at: ${Boost_INCLUDE_DIRS}")
else()
    message(FATAL_ERROR "Boost.Context not found!")
endif()

# 编译选项
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -Wall -Wextra")

# 创建可执行文件
add_executable(main main.cpp)

# 包含目录
target_include_directories(main PRIVATE ${Boost_INCLUDE_DIRS})

# 链接库
target_link_libraries(main 
    PRIVATE 
    Threads::Threads 
    Boost::context
)