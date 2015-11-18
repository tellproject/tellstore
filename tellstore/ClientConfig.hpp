/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
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
            : maxPendingResponses(48ull),
              maxBatchSize(16ull),
              numNetworkThreads(2ull) {
        infinibandConfig.receiveBufferCount = 256;
        infinibandConfig.sendBufferCount = 256;
        infinibandConfig.bufferLength = 128 * 1024;
        infinibandConfig.sendQueueLength = 128;
        infinibandConfig.completionQueueLength = 512;
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
