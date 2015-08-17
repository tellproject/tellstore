###################
# TellStore Delta Main implementation
###################
set(DELTAMAIN_SRCS
    Record.cpp
    Table.cpp
    rowstore/RowStoreVersionIterator.cpp
    rowstore/RowStorePage.cpp
    rowstore/RowStoreScanProcessor.cpp
    colstore/ColumnMapPage.cpp
    colstore/ColumnMapScanProcessor.cpp
)

set(DELTAMAIN_PRIVATE_HDR
    InsertMap.hpp
    Record.hpp
    LogRecord.hpp
    rowstore/RowStoreRecord.hpp
    rowstore/RowStoreVersionIterator.hpp
    rowstore/RowStorePage.hpp
    rowstore/RowStoreScanProcessor.hpp
#    colstore/ColumnMapUtils.hpp
    colstore/ColumnMapRecord.hpp
    colstore/ColumnMapPage.hpp
    colstore/ColumnMapScanProcessor.hpp
    Table.hpp
    DeltaMainRewriteStore.hpp
)

# Add TellStore delta main library
add_library(tellstore-deltamain STATIC ${DELTAMAIN_SRCS} ${DELTAMAIN_PRIVATE_HDR})
target_include_directories(tellstore-deltamain PUBLIC ${PROJECT_SOURCE_DIR})

# Link against TellStore util
target_link_libraries(tellstore-deltamain PUBLIC tellstore-util)
