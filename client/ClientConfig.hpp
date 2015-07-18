#pragma once

#include <crossbow/infinio/InfinibandLimits.hpp>
#include <crossbow/string.hpp>

#include <cstdint>

namespace tell {
namespace store {

struct ClientConfig {
    /// Address of the server to connect to
    crossbow::string commitManager = "";

    /// Port to connect to the server
    uint16_t commitManagerPort = 7242;

    /// Address of the server to connect to
    crossbow::string server = "";

    /// Port to connect to the server
    uint16_t port = 7241;

    /// Number of network threads to process transactions on
    int numNetworkThreads = 2;

    /// Size of memory region to reserve for scans
    size_t scanMemory = 0x80000000ull;

    /// Number of tuples to insert per transaction
    size_t numTuple = 1000000ull;

    /// Number of concurrent transactions to start
    size_t numTransactions = 10;

    /// Configuration limits for the Infiniband device
    crossbow::infinio::InfinibandLimits infinibandLimits;
};

} // namespace store
} // namespace tell
