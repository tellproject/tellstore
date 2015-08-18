###################
# TellStore client library
###################
set(CLIENT_SRCS
    ClientManager.cpp
    ClientSocket.cpp
    ScanMemory.cpp
    Table.cpp
)

set(CLIENT_PUBLIC_HDR
    ClientConfig.hpp
    ClientManager.hpp
    ClientSocket.hpp
    ScanMemory.hpp
    Table.hpp
)

# Transform public header list to use absolute paths
foreach(hdr ${CLIENT_PUBLIC_HDR})
    list(APPEND CLIENT_PUBLIC_HDR_ABSOLUTE ${PROJECT_SOURCE_DIR}/tellstore/${hdr})
endforeach()

# Retrieve the public header list for the common library (client has to install those headers)
get_target_property(COMMON_PUBLIC_HDR_ABSOLUTE tellstore-common PUBLIC_HEADER)

# Add TellStore client library
add_library(tellstore-client ${CLIENT_SRCS} ${CLIENT_PUBLIC_HDR_ABSOLUTE})
set_target_properties(tellstore-client PROPERTIES PUBLIC_HEADER
        "${COMMON_PUBLIC_HDR_ABSOLUTE};${CLIENT_PUBLIC_HDR_ABSOLUTE}")
target_include_directories(tellstore-client PUBLIC $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/${INCLUDE_INSTALL_DIR}>
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>)

# Workaround for link failure with GCC 5 (GCC Bug 65913)
if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
    target_link_libraries(tellstore-client PUBLIC atomic)
endif()

# Link against TellStore common
target_link_libraries(tellstore-client PRIVATE tellstore-common)

# Link against CommitManager
target_link_libraries(tellstore-client PUBLIC commitmanager-client)

# Link against Boost
target_include_directories(tellstore-client PUBLIC ${Boost_INCLUDE_DIRS})

# Link against Crossbow
target_include_directories(tellstore-client PRIVATE ${Crossbow_INCLUDE_DIRS})
target_link_libraries(tellstore-client PUBLIC crossbow_infinio crossbow_logger)

# Install the library
install(TARGETS tellstore-client EXPORT TellStoreClientTargets
        ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
        LIBRARY DESTINATION ${LIB_INSTALL_DIR}
        PUBLIC_HEADER DESTINATION ${INCLUDE_INSTALL_DIR}/tellstore)

# Install the cmake target
install(EXPORT TellStoreClientTargets DESTINATION ${CMAKE_INSTALL_DIR})
