###################
# TellStore Logstructured implementation
###################
set(LOGSTRUCTURED_SRCS
    GcScanProcessor.cpp
    HashScanProcessor.cpp
    Table.cpp
    VersionRecordIterator.cpp
)

set(LOGSTRUCTURED_PRIVATE_HDR
    ChainedVersionRecord.hpp
    GcScanProcessor.hpp
    HashScanProcessor.hpp
    LogstructuredMemoryStore.hpp
    Table.hpp
    VersionRecordIterator.hpp
)

# Add TellStore logstructured library
add_library(tellstore-logstructured STATIC ${LOGSTRUCTURED_SRCS} ${LOGSTRUCTURED_PRIVATE_HDR})
target_include_directories(tellstore-logstructured PUBLIC ${PROJECT_SOURCE_DIR})

# Link against TellStore util
target_link_libraries(tellstore-logstructured PUBLIC tellstore-util)
