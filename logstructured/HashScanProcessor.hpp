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

#include <util/ScanQuery.hpp>
#include <util/StoreImpl.hpp>

#include <crossbow/non_copyable.hpp>

#include <cstdint>
#include <vector>

namespace tell {
namespace store {
namespace logstructured {

class HashScanGarbageCollector;
class Table;

/**
 * @brief Scan processor for the Log-Structured Memory approach scanning over the hash table
 */
class HashScanProcessor : crossbow::non_copyable {
public:
    using GarbageCollector = HashScanGarbageCollector;

    static std::vector<HashScanProcessor> startScan(Table& table, size_t numThreads, const char* queryBuffer,
            const std::vector<ScanQuery*>& queries);

    HashScanProcessor(Table& table, size_t start, size_t end, const char* queryBuffer,
            const std::vector<ScanQuery*>& queryData, uint64_t minVersion);

    HashScanProcessor(HashScanProcessor&& other);

    /**
     * @brief Scans over all entries in the hash table
     *
     * Processes all valid entries with the associated scan queries.
     */
    void process();

private:
    Table& mTable;
    ScanQueryBatchProcessor mQueries;
    uint64_t mMinVersion;

    size_t mStart;
    size_t mEnd;
};

/**
 * @brief Garbage collector for the Log-Structured Memory approach scanning over the hash table
 */
class HashScanGarbageCollector {
public:
    HashScanGarbageCollector(StoreImpl<Implementation::LOGSTRUCTURED_MEMORY>&)
    {}

    void run(const std::vector<Table*>& tables, uint64_t minVersion);
};

} // namespace logstructured
} // namespace store
} // namespace tell
