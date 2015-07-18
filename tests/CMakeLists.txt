###################
# Unit tests
###################
set(TEST_SRCS
    DummyCommitManager.cpp
    DummyCommitManager.hpp
    testCuckooMap.cpp
    testCommitManager.cpp
    testLog.cpp
    testOpenAddressingHash.cpp
    testScanQuery.cpp
    simpleTests.cpp
    logstructured/testTable.cpp
)

set(TEST_PRIVATE_HDR
    DummyCommitManager.hpp
)

# Add test executable
add_executable(tests main.cpp ${TEST_SRCS} ${TEST_PRIVATE_HDR})
target_include_directories(tests PRIVATE ${PROJECT_BINARY_DIR})
target_link_libraries(tests PRIVATE tellstore-deltamain tellstore-logstructured)

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

###################
# TellStore client test
###################
set(CLIENT_TEST_SRCS
    client/TestClient.cpp
)

# Add TellStore test client executable
add_executable(tellstore-test ${CLIENT_TEST_SRCS})

# Link against TellStore client
target_link_libraries(tellstore-test PRIVATE tellstore-client)

# Link against Crossbow
target_include_directories(tellstore-test PRIVATE ${Crossbow_INCLUDE_DIRS})
target_link_libraries(tellstore-test PRIVATE crossbow_infinio crossbow_logger)

# Link against Jemalloc
target_include_directories(tellstore-test PRIVATE ${Jemalloc_INCLUDE_DIRS})
target_link_libraries(tellstore-test PRIVATE ${Jemalloc_LIBRARIES})
