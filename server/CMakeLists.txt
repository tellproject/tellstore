###################
# TellStore server
###################
set(SERVER_SRCS
    ClientConnection.hpp
    ClientConnection.cpp
    ConnectionManager.hpp
    ConnectionManager.cpp
    ServerConfig.hpp
)

# Add TellStore server executable
add_executable(tellstored main.cpp ${SERVER_SRCS})

# Link against TellStore library
target_link_libraries(tellstored PRIVATE TellStoreUtil TellStoreNetwork TellStore)

# Link against Boost
target_include_directories(tellstored PRIVATE ${Boost_INCLUDE_DIRS})

# Link against Crossbow
target_include_directories(tellstored PRIVATE ${Crossbow_INCLUDE_DIRS})
target_link_libraries(tellstored PRIVATE crossbow_infinio)
