#pragma once

#include <crossbow/infinio/Endpoint.hpp>
#include <crossbow/infinio/InfinibandLimits.hpp>

#include <cstdint>
#include <vector>

namespace tell {
namespace store {

struct ClientConfig {
    /// Address of the CommitManager to connect to
    crossbow::infinio::Endpoint commitManager;

    /// Address of the TellStore to connect to
    std::vector<crossbow::infinio::Endpoint> tellStore;

    /// Maximum number of concurrent pending network requests (per connection)
    size_t maxPendingResponses = 100ull;

    /// Number of network threads to process transactions on
    size_t numNetworkThreads = 2ull;
};

} // namespace store
} // namespace tell
