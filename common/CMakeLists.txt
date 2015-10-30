###################
# TellStore common
###################
set(COMMON_SRCS
    GenericTuple.cpp
    MessageTypes.cpp
    Record.cpp
)

set(COMMON_PUBLIC_HDR
    AbstractTuple.hpp
    ErrorCode.hpp
    GenericTuple.hpp
    MessageTypes.hpp
    Record.hpp
)

# Transform public header list to use absolute paths
foreach(hdr ${COMMON_PUBLIC_HDR})
    list(APPEND COMMON_PUBLIC_HDR_ABSOLUTE ${PROJECT_SOURCE_DIR}/tellstore/${hdr})
endforeach()

# Add TellStore common library
add_library(tellstore-common STATIC ${COMMON_SRCS} ${COMMON_PUBLIC_HDR_ABSOLUTE})
target_include_directories(tellstore-common PUBLIC ${PROJECT_SOURCE_DIR})
set_target_properties(tellstore-common PROPERTIES PUBLIC_HEADER "${COMMON_PUBLIC_HDR_ABSOLUTE}")

# Link against Crossbow
target_include_directories(tellstore-common PUBLIC ${Crossbow_INCLUDE_DIRS})
target_link_libraries(tellstore-common PUBLIC crossbow_logger)
