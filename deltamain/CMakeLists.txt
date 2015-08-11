###################
# TellStore Delta Main implementation
###################
set(DELTAMAIN_SRCS
    Page.cpp
    Record.cpp
    Table.cpp
#    RowStoreRecord.cpp
#    ColumnMapRecord.cpp
)

set(DELTAMAIN_PRIVATE_HDR
    InsertMap.hpp
    Page.hpp
    Record.hpp
    LogRecord.hpp
    rowstore/RowStoreRecord.hpp
    colstore/ColumnMapRecord.hpp
    Table.hpp
    DeltaMainRewriteStore.hpp
)

# Add TellStore delta main library
add_library(tellstore-deltamain STATIC ${DELTAMAIN_SRCS} ${DELTAMAIN_PRIVATE_HDR})
target_include_directories(tellstore-deltamain PUBLIC ${PROJECT_SOURCE_DIR})

# Link against TellStore util
target_link_libraries(tellstore-deltamain PUBLIC tellstore-util)
