###################
# TellStore server
###################
set(SERVER_SRCS
    ServerScanQuery.cpp
    ServerSocket.cpp
)

set(SERVER_PRIVATE_HDR
    ServerConfig.hpp
    ServerScanQuery.hpp
    ServerSocket.hpp
    Storage.hpp
)

# Add TellStore server executable
add_executable(tellstored main.cpp ${SERVER_SRCS} ${SERVER_PRIVATE_HDR})

# Link against TellStore library
target_link_libraries(tellstored PRIVATE ${STORAGE_LIBRARY} tellstore-common)

# Link against Boost
target_include_directories(tellstored PRIVATE ${Boost_INCLUDE_DIRS})

# Link against Crossbow
target_include_directories(tellstored PRIVATE ${Crossbow_INCLUDE_DIRS})
target_link_libraries(tellstored PRIVATE crossbow_allocator crossbow_infinio crossbow_logger)

# Link against Jemalloc
target_include_directories(tellstored PRIVATE ${Jemalloc_INCLUDE_DIRS})
target_link_libraries(tellstored PRIVATE ${Jemalloc_LIBRARIES})

# Install the binary
install(TARGETS tellstored
        RUNTIME DESTINATION ${BIN_INSTALL_DIR})
