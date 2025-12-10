add_executable(afl_store_fuzz afl_store_fuzz.cpp)
target_include_directories(afl_store_fuzz
                           PRIVATE
                           ${ELOQ_STORE_INCLUDE}
                           ${CMAKE_SOURCE_DIR})
target_link_libraries(afl_store_fuzz PRIVATE eloqstore stdc++fs stdc++)
