cmake_minimum_required(VERSION 2.8.12)
project(TellStore)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")

# Declare build options
option(USE_ASAN "Use the address sanitizer" OFF)
option(ENABLE_HEAVYWEIGHT_TESTS "Build ressource intensive or long running unit tests" OFF)

set(IMPLEMENTATION "DELTA_MAIN_REWRITE" CACHE STRING "The approach to use in the implementation")
set(PAGE_SIZE "0x800000" CACHE STRING "The pagesize to use in bytes")
set(TOTAL_MEMORY "0x80000000" CACHE STRING "The pagesize to use in bytes")
set(MAX_QUERY_SHARING "1024" CACHE STRING "The maximal number of queries a scan query accepts")
set(HASHMAP_CAPACITY "0x800000" CACHE STRING "Number of elements to allocate for the hashmap")
set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

# Set compile options
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
if ( USE_ASAN )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -fno-optimize-sibling-calls -fsanitize=address")
endif()
if ( ENABLE_HEAVYWEIGHT_TESTS )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DENABLE_HEAVYWEIGHT_TESTS")
endif()

# The cx16 flag is required for GCC to enable 128 bit atomics
if (CMAKE_COMPILER_IS_GNUCXX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcx16")
endif()

# Find dependencies
find_package(Boost REQUIRED)
find_package(Crossbow REQUIRED)
find_package(TBB REQUIRED)
find_package(Jemalloc REQUIRED)

set(SOURCE_FILES
    implementation.hpp
    tellstore.hpp
    deltamain/Record.hpp
    deltamain/Record.cpp
    deltamain/Table.hpp
    deltamain/Table.cpp
    deltamain/Page.hpp
    deltamain/Page.cpp
    logstructured/Record.hpp
    logstructured/Table.cpp
    logstructured/Table.hpp
    util/OpenAddressingHash.hpp
    util/OpenAddressingHash.cpp
    util/LogOperations.hpp
    util/LogOperations.cpp
    util/NonCopyable.hpp
    util/Log.hpp
    util/Log.cpp
    util/TransactionImpl.hpp
    util/CommitManager.hpp
    util/CommitManager.cpp
    util/PageManager.cpp
    util/PageManager.hpp
    util/CuckooHash.cpp
    util/CuckooHash.hpp
    util/FixedSizeStack.hpp
    util/Epoch.cpp
    util/Epoch.hpp
    util/TableManager.hpp
    util/Record.hpp
    util/Record.cpp
    util/Logging.hpp
    util/Logging.cpp
    util/StorageConfig.hpp
    util/SnapshotDescriptor.hpp
    util/Request.hpp
    util/chunk_allocator.hpp
    util/functional.hpp)

# Create configuration file
configure_file(config.h.in ${CMAKE_CURRENT_BINARY_DIR}/config.h)

# Add TellStore library
add_library(TellStore STATIC ${SOURCE_FILES} ${CMAKE_CURRENT_BINARY_DIR}/config.h)
target_include_directories(TellStore PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(TellStore PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

# Link against Boost
target_include_directories(TellStore PUBLIC ${Boost_INCLUDE_DIRS})

# Link against Crossbow
target_include_directories(TellStore PUBLIC ${Crossbow_INCLUDE_DIRS})

# Link against TBB
target_include_directories(TellStore PUBLIC ${TBB_INCLUDE_DIRS})
target_link_libraries(TellStore PUBLIC ${TBB_LIBRARIES})

# Link against Jemalloc
target_include_directories(TellStore PUBLIC ${Jemalloc_INCLUDE_DIRS})
target_link_libraries(TellStore PUBLIC ${Jemalloc_LIBRARIES})
target_compile_definitions(TellStore PUBLIC JEMALLOC_NO_DEMANGLE)

###################
# GTEST
###################
add_subdirectory(externals/gtest)
enable_testing()

###################
# Unit tests
###################
set(TEST_SRCS
    tests/testCuckooMap.cpp
    tests/testCommitManager.cpp
    tests/testLog.cpp
    tests/testOpenAddressingHash.cpp
    tests/simpleTests.cpp
    tests/logstructured/testTable.cpp)

# Add test executable
add_executable(tests tests/main.cpp ${TEST_SRCS})
target_include_directories(tests PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(tests PRIVATE TellStore)

# Link test against GTest
target_include_directories(tests PRIVATE ${gtest_SOURCE_DIR}/include)
target_link_libraries(tests PRIVATE gtest gtest_main)

add_test(tests tests)
