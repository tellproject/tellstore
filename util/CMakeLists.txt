###################
# TellStore Util library
###################
set(UTIL_SRCS
    CuckooHash.cpp
    Log.cpp
    OpenAddressingHash.cpp
    PageManager.cpp
    ScanQuery.cpp
    LLVMCodeGenerator.cpp
)

set(UTIL_PRIVATE_HDR
    CuckooHash.hpp
    functional.hpp
    Log.hpp
    OpenAddressingHash.hpp
    PageManager.hpp
    PredicateBitmap.hpp
    Scan.hpp
    ScanQuery.hpp
    StorageConfig.hpp
    TableManager.hpp
    UnsafeAtomic.hpp
    VersionManager.hpp
    LLVMCodeGenerator.hpp
    LLVMJIT.hpp
)

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

# Add TellStore util library
add_library(tellstore-util STATIC ${UTIL_SRCS} ${UTIL_PRIVATE_HDR} ${PROJECT_BINARY_DIR}/config.h)

target_include_directories(tellstore-util PUBLIC ${PROJECT_SOURCE_DIR})
target_include_directories(tellstore-util PUBLIC ${PROJECT_BINARY_DIR})

# Workaround for link failure with GCC 5 (GCC Bug 65913)
if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
    target_link_libraries(tellstore-util PUBLIC atomic)
endif()

# Link against TellStore common library
target_link_libraries(tellstore-util PUBLIC tellstore-common)

# Link against CommitManager library
target_link_libraries(tellstore-util PUBLIC commitmanager-client)

# Link against Boost
target_include_directories(tellstore-util PUBLIC ${Boost_INCLUDE_DIRS})

# Link against Crossbow
target_include_directories(tellstore-util PUBLIC ${Crossbow_INCLUDE_DIRS})
target_link_libraries(tellstore-util PUBLIC crossbow_allocator crossbow_logger)

# Link against TBB
target_include_directories(tellstore-util PUBLIC ${TBB_INCLUDE_DIRS})
target_link_libraries(tellstore-util PUBLIC ${TBB_LIBRARIES})

# Link against LLVM
target_link_libraries(tellstore-util PUBLIC ${USED_LLVM_LIBRARIES})
target_include_directories(tellstore-util PUBLIC ${LLVM_INCLUDE_DIRS})
target_compile_definitions(tellstore-util PUBLIC ${LLVM_DEFINITIONS})
