#pragma once

#include <crossbow/infinio/InfinibandLimits.hpp>

#include <cstddef>
#include <cstdint>

namespace tell {
namespace store {

/**
 * @brief The ServerConfig struct containing configuration parameters for the TellStore server
 */
struct ServerConfig {
    /// Port to listen for incoming client connections
    uint16_t port = 7241;

    /// Configuration limits for the Infiniband device
    crossbow::infinio::InfinibandLimits infinibandLimits;
};

} // namespace store
} // namespace tell
