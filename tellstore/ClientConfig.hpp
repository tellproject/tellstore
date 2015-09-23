#pragma once

#include <crossbow/infinio/Endpoint.hpp>
#include <crossbow/infinio/InfinibandLimits.hpp>
#include <crossbow/string.hpp>

#include <cstdint>
#include <vector>

namespace tell {
namespace store {

struct ClientConfig {
    static crossbow::infinio::Endpoint parseCommitManager(const crossbow::string& host) {
        return crossbow::infinio::Endpoint(crossbow::infinio::Endpoint::ipv4(), host);
    }

    static inline std::vector<crossbow::infinio::Endpoint> parseTellStore(const crossbow::string& host);

    ClientConfig()
            : maxPendingResponses(100ull),
              maxBatchSize(16ull),
              numNetworkThreads(2ull) {
        infinibandConfig.receiveBufferCount = 128;
        infinibandConfig.sendBufferCount = 128;
        infinibandConfig.bufferLength = 32 * 1024;
        infinibandConfig.sendQueueLength = 128;
    }

    /// Configuration for the Infiniband devices
    crossbow::infinio::InfinibandLimits infinibandConfig;

    /// Address of the CommitManager to connect to
    crossbow::infinio::Endpoint commitManager;

    /// Address of the TellStore to connect to
    std::vector<crossbow::infinio::Endpoint> tellStore;

    /// Maximum number of concurrent pending network requests (per connection)
    size_t maxPendingResponses;

    /// Maximum number of messages per batch
    size_t maxBatchSize;

    /// Number of network threads to process transactions on
    size_t numNetworkThreads;
};

std::vector<crossbow::infinio::Endpoint> ClientConfig::parseTellStore(const crossbow::string& host) {
    if (host.empty()) {
        return {};
    }

    std::vector<crossbow::infinio::Endpoint> result;
    size_t i = 0;
    while (true) {
        auto pos = host.find(';', i);
        result.emplace_back(crossbow::infinio::Endpoint::ipv4(), host.substr(i, pos));
        if (pos == crossbow::string::npos) {
            break;
        }
        i = pos + 1;
    }
    return result;
}

} // namespace store
} // namespace tell
