cmake_minimum_required(VERSION 2.8.12)
project(TellStore)

set(PAGE_SIZE "0x200000" CACHE STRING "The pagesize to use in bytes")
set(TOTAL_MEMORY "0x80000000" CACHE STRING "The pagesize to use in bytes")
set(MAX_QUERY_SHARING "1024" CACHE STRING "The maximal number of queries a scan query accepts")
set(HASHMAP_CAPACITY "0x800000" CACHE STRING "Number of elements to allocate for the hashmap")

# Set default install paths
set(BIN_INSTALL_DIR bin CACHE PATH "Installation directory for binaries")
set(CMAKE_INSTALL_DIR cmake CACHE PATH "Installation directory for CMake files")
set(INCLUDE_INSTALL_DIR include CACHE PATH "Installation directory for header files")
set(LIB_INSTALL_DIR lib CACHE PATH "Installation directory for libraries")

# Set the TellStore directory
set(TellStore_DIR ${CMAKE_CURRENT_BINARY_DIR} CACHE PATH "Path to the TellStore binaries and configuration")

# Set CMake modules path
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# Set compile options
# The cx16 flag is required for GCC to enable 128 bit atomics
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -mcx16")

# Find dependencies
find_package(Boost REQUIRED)
find_package(Crossbow COMPONENTS Allocator InfinIO Logger REQUIRED)
find_package(CommitManager REQUIRED)
find_package(Sparsehash REQUIRED)
find_package(TBB REQUIRED)
find_package(Jemalloc REQUIRED)
find_package(LLVM 3.7 EXACT REQUIRED)

# Create configuration file
configure_file(config.h.in ${PROJECT_BINARY_DIR}/config.h)

add_subdirectory(common)
add_subdirectory(util)
add_subdirectory(deltamain)
add_subdirectory(logstructured)
add_subdirectory(server)
add_subdirectory(client)

###################
# GTEST
###################
add_subdirectory(externals/gtest)

enable_testing()
add_subdirectory(tests)

# Create cmake config file
configure_file(TellStoreConfig.cmake.in ${CMAKE_CURRENT_BINARY_DIR}/TellStoreConfig.cmake @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/TellStoreConfig.cmake DESTINATION ${CMAKE_INSTALL_DIR})
