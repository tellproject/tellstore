###################
# Unit tests
###################
set(TEST_SRCS
    testCuckooMap.cpp
    testCommitManager.cpp
    testLog.cpp
    testOpenAddressingHash.cpp
    testScanQuery.cpp
    simpleTests.cpp
    logstructured/testTable.cpp)

# Add test executable
add_executable(tests main.cpp ${TEST_SRCS})
target_include_directories(tests PRIVATE ${PROJECT_BINARY_DIR})
target_link_libraries(tests PRIVATE TellStore)

# Link test against GTest
target_include_directories(tests PRIVATE ${gtest_SOURCE_DIR}/include)
target_link_libraries(tests PRIVATE gtest gtest_main)

# Link against Crossbow
target_include_directories(tests PRIVATE ${Crossbow_INCLUDE_DIRS})
target_link_libraries(tests PRIVATE crossbow_allocator)

# Link against Jemalloc
target_include_directories(tests PRIVATE ${Jemalloc_INCLUDE_DIRS})
target_link_libraries(tests PRIVATE ${Jemalloc_LIBRARIES})

add_test(tests tests)
