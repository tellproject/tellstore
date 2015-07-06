###################
# TellStore Network library
###################
set(NETWORK_SRCS
    ErrorCode.hpp
    MessageSocket.hpp
    MessageTypes.hpp
)

# Add TellStore network library
add_library(TellStoreNetwork STATIC ${NETWORK_SRCS})
target_include_directories(TellStoreNetwork PUBLIC ${PROJECT_SOURCE_DIR})

# Link against TellStore library
target_link_libraries(TellStoreNetwork PUBLIC TellStoreUtil)

# Link against Crossbow
target_include_directories(TellStoreNetwork PUBLIC ${Crossbow_INCLUDE_DIRS})
target_link_libraries(TellStoreNetwork PUBLIC crossbow_infinio crossbow_logger)
