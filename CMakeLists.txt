cmake_minimum_required(VERSION 2.8.12)
project(TellStore)

set(IMPLEMENTATION "LOGSTRUCTURED_MEMORY" CACHE STRING "The approach to use in the implementation")
set(STORAGE_LAYOUT "ROW_STORE" CACHE STRING "The storage layout to use in the implementation")
set(SCAN_PROCESSOR "LOG" CACHE STRING "The scan processor to use in the implementation")
set(PAGE_SIZE "0x800000" CACHE STRING "The pagesize to use in bytes")
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

if(IMPLEMENTATION STREQUAL "DELTA_MAIN_REWRITE")
    set(STORAGE_LIBRARY tellstore-deltamain)
    set(USE_DELTA_MAIN_REWRITE 1)
elseif(IMPLEMENTATION STREQUAL "LOGSTRUCTURED_MEMORY")
    set(STORAGE_LIBRARY tellstore-logstructured)
    set(USE_LOGSTRUCTURED_MEMORY 1)
else()
    message(FATAL_ERROR "Unknown implementation")
endif()

if(STORAGE_LAYOUT STREQUAL "ROW_STORE")
    set(USE_ROW_STORE 1)
elseif(STORAGE_LAYOUT STREQUAL "COLUMN_MAP")
    set(USE_COLUMN_MAP 1)
else()
    message(FATAL_ERROR "Unknown storage layout")
endif()

if(SCAN_PROCESSOR STREQUAL "LOG")
    set(USE_LOG_SCAN 1)
elseif(SCAN_PROCESSOR STREQUAL "HASH")
    set(USE_HASH_SCAN 1)
else()
    message(FATAL_ERROR "Unknown scan processor")
endif()

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
