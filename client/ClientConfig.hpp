#pragma once

#include <crossbow/infinio/InfinibandLimits.hpp>
#include <crossbow/string.hpp>

#include <cstdint>

namespace tell {
namespace store {

struct ClientConfig {
    /// Address of the server to connect to
    crossbow::string server = "";

    /// Port to connect to the server
    uint16_t port = 7241;

    /// Configuration limits for the Infiniband device
    crossbow::infinio::InfinibandLimits infinibandLimits;
};

} // namespace store
} // namespace tell
