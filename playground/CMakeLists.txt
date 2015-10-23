cmake_minimum_required(VERSION 2.8.12)
project(Playground)

# Set CMake modules path
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/../cmake)

# Set compile options
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -march=native -O3")

# Set external libraries
find_package(LLVM REQUIRED)

# Executable
add_executable(Playground llvm.cpp)

target_link_libraries(Playground LLVMCore LLVMSupport)


