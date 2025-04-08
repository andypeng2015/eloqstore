project(DBStress)

find_package(gflags REQUIRED)

add_executable(db_stress ${CMAKE_SOURCE_DIR}/db_stress/db_stress.cpp)

target_include_directories(db_stress PUBLIC ${CMAKE_SOURCE_DIR} ${INCLUDE_DIR})

target_link_libraries(db_stress PRIVATE gflags test_util)
