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
