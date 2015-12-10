###################
# TellStore Delta Main implementation
###################
set(DELTAMAIN_SRCS
    InsertHash.cpp
    Record.cpp
    Table.cpp
    colstore/ColumnMapContext.cpp
    colstore/ColumnMapPage.cpp
    colstore/ColumnMapRecord.cpp
    colstore/ColumnMapScanProcessor.cpp
    colstore/LLVMColumnMapScan.cpp
    colstore/LLVMColumnMapUtils.cpp
    rowstore/RowStorePage.cpp
    rowstore/RowStoreRecord.cpp
    rowstore/RowStoreScanProcessor.cpp
)

set(DELTAMAIN_PRIVATE_HDR
    DeltaMainRewriteStore.hpp
    InsertHash.hpp
    Record.hpp
    Table.hpp
    colstore/ColumnMapContext.hpp
    colstore/ColumnMapPage.hpp
    colstore/ColumnMapRecord.hpp
    colstore/ColumnMapScanProcessor.hpp
    colstore/LLVMColumnMapUtils.hpp
    colstore/LLVMColumnMapScan.hpp
    rowstore/RowStoreContext.hpp
    rowstore/RowStorePage.hpp
    rowstore/RowStoreRecord.hpp
    rowstore/RowStoreScanProcessor.hpp
)

# Add TellStore delta main library
add_library(tellstore-deltamain STATIC ${DELTAMAIN_SRCS} ${DELTAMAIN_PRIVATE_HDR})
target_include_directories(tellstore-deltamain PUBLIC ${PROJECT_SOURCE_DIR})

# Link against TellStore util
target_link_libraries(tellstore-deltamain PUBLIC tellstore-util)
