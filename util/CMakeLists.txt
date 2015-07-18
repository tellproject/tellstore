###################
# TellStore Util library
###################
set(UTIL_SRCS
    CuckooHash.cpp
    Log.cpp
    LogOperations.cpp
    OpenAddressingHash.cpp
    PageManager.cpp
)

set(UTIL_PRIVATE_HDR
    chunk_allocator.hpp
    OpenAddressingHash.hpp
    CuckooHash.hpp
    FixedSizeStack.hpp
    functional.hpp
    IteratorEntry.hpp
    Log.hpp
    LogOperations.hpp
    OpenAddressingHash.hpp
    PageManager.hpp
    Scan.hpp
    ScanQuery.hpp
    StorageConfig.hpp
    StoreImpl.hpp
    TableManager.hpp
)

# Add TellStore util library
add_library(tellstore-util STATIC ${UTIL_SRCS} ${UTIL_PRIVATE_HDR} ${PROJECT_BINARY_DIR}/config.h)

target_include_directories(tellstore-util PUBLIC ${PROJECT_SOURCE_DIR})
target_include_directories(tellstore-util PUBLIC ${PROJECT_BINARY_DIR})

# Workaround for link failure with GCC 5.1 (GCC Bug 65913)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_EQUAL "5.1.0")
        target_link_libraries(tellstore-util PUBLIC atomic)
    endif()
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
