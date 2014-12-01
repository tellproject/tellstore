cmake_minimum_required(VERSION 2.8.4)
project(TellStore)

include_directories(${CMAKE_SOURCE_DIR})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(IMPLEMENTATION "DELTA_MAIN_REWRITE" CACHE STRING "The approach to use in the implementation")
set(PAGE_SIZE "0x800000" CACHE STRING "The pagesize to use in bytes")
set(TOTAL_MEMORY "0x80000000" CACHE STRING "The pagesize to use in bytes")

find_package(Crossbow REQUIRED)
include_directories(${Crossbow_INCLUDE_DIRS})

configure_file("${CMAKE_SOURCE_DIR}/config.h.in" "${CMAKE_BINARY_DIR}/config.h")
include_directories(${CMAKE_BINARY_DIR})

set(SOURCE_FILES
    main.cpp
    implementation.hpp
    tellstore.hpp
    deltamainrewrite/dmrewrite.hpp
    deltamainrewrite/dmrewrite.cpp
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
    util/Log.hpp
    util/Log.cpp)
add_executable(TellStore ${SOURCE_FILES})
