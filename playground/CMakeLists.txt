cmake_minimum_required(VERSION 2.8.12)
project(Playground)

# Set CMake modules path
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/../cmake)

# Set compile options
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -march=native -rdynamic -fPIC -fvisibility-inlines-hidden -ffunction-sections -fdata-sections -fno-exceptions -fno-rtti")

# Set external libraries
find_package(LLVM 3.7 EXACT REQUIRED)

set(USED_LLVM_LIBRARIES
    LLVMX86Disassembler
    LLVMX86AsmParser
    LLVMX86CodeGen
    LLVMSelectionDAG
    LLVMAsmPrinter
    LLVMCodeGen
    LLVMScalarOpts
    LLVMProfileData
    LLVMInstCombine
    LLVMTransformUtils
    LLVMipa
    LLVMAnalysis
    LLVMX86Desc
    LLVMMCDisassembler
    LLVMX86Info
    LLVMX86AsmPrinter
    LLVMX86Utils
    LLVMMCJIT
    LLVMTarget
    LLVMExecutionEngine
    LLVMRuntimeDyld
    LLVMObject
    LLVMMCParser
    LLVMBitReader
    LLVMMC
    LLVMCore
    LLVMSupport
    LLVMVectorize
)

include_directories(
    ../util
)

# Executables
add_executable(lexer-parser-example lexer-parser.cpp)
add_executable(Playground llvm.cpp)

target_link_libraries(lexer-parser-example ${USED_LLVM_LIBRARIES} )
target_link_libraries(Playground ${USED_LLVM_LIBRARIES})


