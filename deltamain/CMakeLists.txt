###################
# TellStore Delta Main implementation
###################
set(DELTAMAIN_SRCS
    InsertHash.cpp
    Record.cpp
    Table.cpp
    colstore/ColumnMapPage.cpp
    colstore/ColumnMapScanProcessor.cpp
    rowstore/RowStorePage.cpp
    rowstore/RowStoreScanProcessor.cpp
    rowstore/RowStoreVersionIterator.cpp
)

set(DELTAMAIN_PRIVATE_HDR
    DeltaMainRewriteStore.hpp
    InsertHash.hpp
    InsertMap.hpp
    LogRecord.hpp
    Record.hpp
    Table.hpp
    colstore/ColumnMapPage.hpp
    colstore/ColumnMapRecord.hpp
    colstore/ColumnMapScanProcessor.hpp
    colstore/ColumnMapUtils.hpp
    rowstore/RowStorePage.hpp
    rowstore/RowStoreRecord.hpp
    rowstore/RowStoreScanProcessor.hpp
    rowstore/RowStoreVersionIterator.cpp
)

# Add TellStore delta main library
add_library(tellstore-deltamain STATIC ${DELTAMAIN_SRCS} ${DELTAMAIN_PRIVATE_HDR})
target_include_directories(tellstore-deltamain PUBLIC ${PROJECT_SOURCE_DIR})

# Link against TellStore util
target_link_libraries(tellstore-deltamain PUBLIC tellstore-util)
