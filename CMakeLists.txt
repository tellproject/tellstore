cmake_minimum_required(VERSION 2.8.12)
project(TellStore)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/")

# Declare build options
option(USE_ASAN "Use the address sanitizer" OFF)

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

# The cx16 flag is required for GCC to enable 128 bit atomics
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcx16")

# Find dependencies
find_package(Boost REQUIRED)
find_package(Crossbow COMPONENTS Allocator InfinIO Logger REQUIRED)
find_package(TBB REQUIRED)
find_package(Jemalloc REQUIRED)

# Create configuration file
configure_file(config.h.in ${PROJECT_BINARY_DIR}/config.h)

add_subdirectory(util)

###################
# TellStore library
###################
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
)

# Add TellStore library
add_library(TellStore STATIC ${SOURCE_FILES} ${PROJECT_BINARY_DIR}/config.h)
target_include_directories(TellStore PUBLIC ${PROJECT_SOURCE_DIR})
target_include_directories(TellStore PUBLIC ${PROJECT_BINARY_DIR})

# Link against TellStore library
target_link_libraries(TellStore PUBLIC TellStoreUtil)

# Link against Crossbow
target_include_directories(TellStore PUBLIC ${Crossbow_INCLUDE_DIRS})

add_subdirectory(network)
add_subdirectory(server)
add_subdirectory(client)

###################
# GTEST
###################
add_subdirectory(externals/gtest)

enable_testing()
add_subdirectory(tests)
