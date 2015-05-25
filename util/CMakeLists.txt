###################
# TellStore Util library
###################
set(UTIL_SRCS
    OpenAddressingHash.hpp
    OpenAddressingHash.cpp
    LogOperations.hpp
    LogOperations.cpp
    NonCopyable.hpp
    Log.hpp
    Log.cpp
    TransactionImpl.hpp
    CommitManager.hpp
    CommitManager.cpp
    PageManager.cpp
    PageManager.hpp
    CuckooHash.cpp
    CuckooHash.hpp
    FixedSizeStack.hpp
    Epoch.cpp
    Epoch.hpp
    TableManager.hpp
    Record.hpp
    Record.cpp
    Logging.hpp
    Logging.cpp
    StorageConfig.hpp
    SnapshotDescriptor.hpp
    Request.hpp
    Scan.hpp
    ScanQuery.hpp
    IteratorEntry.hpp
    chunk_allocator.hpp
    functional.hpp
)

# Add TellStore util library
add_library(TellStoreUtil STATIC ${UTIL_SRCS} ${PROJECT_BINARY_DIR}/config.h)
target_include_directories(TellStoreUtil PUBLIC ${PROJECT_SOURCE_DIR})
target_include_directories(TellStoreUtil PUBLIC ${PROJECT_BINARY_DIR})

# Workaround for link failure with GCC 5.1 (GCC Bug 65913)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_EQUAL "5.1.0")
        target_link_libraries(TellStoreUtil PUBLIC atomic)
    endif()
endif()

# Link against Boost
target_include_directories(TellStoreUtil PUBLIC ${Boost_INCLUDE_DIRS})

# Link against Crossbow
target_include_directories(TellStoreUtil PUBLIC ${Crossbow_INCLUDE_DIRS})

# Link against TBB
target_include_directories(TellStoreUtil PUBLIC ${TBB_INCLUDE_DIRS})
target_link_libraries(TellStoreUtil PUBLIC ${TBB_LIBRARIES})

# Link against Jemalloc
target_include_directories(TellStoreUtil PUBLIC ${Jemalloc_INCLUDE_DIRS})
target_link_libraries(TellStoreUtil PUBLIC ${Jemalloc_LIBRARIES})
target_compile_definitions(TellStoreUtil PUBLIC JEMALLOC_NO_DEMANGLE)
