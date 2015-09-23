#pragma once

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

    /// Number of network threads to process requests on
    int numNetworkThreads = 2;

    /// Size of the buffers used in scans
    uint32_t scanBufferLength = 0x100000;

    /// Maximum number of buffers used in scans
    uint32_t scanBufferCount = 256;

    /// Maximum number of scan buffers that are in flight on a socket at the same time
    /// This limit might be exceeded by a small factor
    uint64_t maxInflightScanBuffer = 16;

    /// Maximum number of messages per batch
    size_t maxBatchSize = 16;
};

} // namespace store
} // namespace tell
