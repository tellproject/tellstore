cmake_minimum_required(VERSION 2.8.12)
project(TellStore)

# For vim autocompletion
set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

# Declare build options
option(USE_ASAN "Use the address sanitizer" OFF)

set(IMPLEMENTATION "DELTA_MAIN_REWRITE" CACHE STRING "The approach to use in the implementation")
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

# Build with PIC by default
if(NOT CMAKE_POSITION_INDEPENDENT_CODE)
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endif()

# Set compile options
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
if (USE_ASAN)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -fno-optimize-sibling-calls -fsanitize=address")
endif()

# The cx16 flag is required for GCC to enable 128 bit atomics
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcx16")

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
