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

macro(add_tellstored _name _implementation)
    # Add TellStore server executable
    add_executable(tellstored-${_name} main.cpp ${SERVER_SRCS} ${SERVER_PRIVATE_HDR})
    target_compile_definitions(tellstored-${_name} PRIVATE ${ARGN})

    # Link against TellStore library
    target_link_libraries(tellstored-${_name} PRIVATE tellstore-${_implementation} tellstore-common)

    # Link against Boost
    target_include_directories(tellstored-${_name} PRIVATE ${Boost_INCLUDE_DIRS})

    # Link against Crossbow
    target_include_directories(tellstored-${_name} PRIVATE ${Crossbow_INCLUDE_DIRS})
    target_link_libraries(tellstored-${_name} PRIVATE crossbow_allocator crossbow_infinio crossbow_logger)

    # Link against Jemalloc
    target_include_directories(tellstored-${_name} PRIVATE ${Jemalloc_INCLUDE_DIRS})
    target_link_libraries(tellstored-${_name} PRIVATE ${Jemalloc_LIBRARIES})

    # Install the binary
    install(TARGETS tellstored-${_name}
            RUNTIME DESTINATION ${BIN_INSTALL_DIR})
endmacro()

add_tellstored(logstructured logstructured USE_LOGSTRUCTURED_MEMORY)
add_tellstored(rowstore deltamain USE_DELTA_MAIN_REWRITE USE_ROW_STORE)
add_tellstored(columnmap deltamain USE_DELTA_MAIN_REWRITE USE_COLUMN_MAP)
