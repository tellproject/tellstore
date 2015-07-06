###################
# TellStore client
###################
set(CLIENT_SRCS
    Client.hpp
    Client.cpp
    ClientConfig.hpp
    ServerConnection.hpp
    ServerConnection.cpp
    TransactionManager.hpp
    TransactionManager.cpp
)

# Add TellStore client executable
add_executable(tellstore-client main.cpp ${CLIENT_SRCS})

# Link against TellStore library
target_link_libraries(tellstore-client PRIVATE TellStoreUtil TellStoreNetwork)

# Link against Boost
target_include_directories(tellstore-client PRIVATE ${Boost_INCLUDE_DIRS})
target_link_libraries(tellstore-client PRIVATE ${Boost_LIBRARIES})

# Link against Crossbow
target_include_directories(tellstore-client PRIVATE ${Crossbow_INCLUDE_DIRS})
target_link_libraries(tellstore-client PRIVATE crossbow_allocator crossbow_infinio)

# Link against Jemalloc
target_include_directories(tellstore-client PRIVATE ${Jemalloc_INCLUDE_DIRS})
target_link_libraries(tellstore-client PRIVATE ${Jemalloc_LIBRARIES})
