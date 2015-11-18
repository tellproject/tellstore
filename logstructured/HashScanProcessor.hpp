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

#include <util/LLVMScan.hpp>
#include <util/ScanQuery.hpp>

#include <crossbow/allocator.hpp>
#include <crossbow/non_copyable.hpp>

#include <cstdint>
#include <vector>

namespace tell {
namespace store {

struct LogstructuredMemoryStore;

namespace logstructured {

class HashScanGarbageCollector;
class HashScanProcessor;
class Table;

class HashScan : public LLVMRowScanBase {
public:
    using ScanProcessor = HashScanProcessor;
    using GarbageCollector = HashScanGarbageCollector;

    HashScan(Table* table, std::vector<ScanQuery*> queries);

    std::vector<std::unique_ptr<HashScanProcessor>> startScan(size_t numThreads);

private:
    Table* mTable;

    crossbow::allocator mAllocator;
};

/**
 * @brief Scan processor for the Log-Structured Memory approach scanning over the hash table
 */
class HashScanProcessor : public LLVMRowScanProcessorBase {
public:
    HashScanProcessor(Table& table, const std::vector<ScanQuery*>& queries, size_t start, size_t end,
            uint64_t minVersion, HashScan::RowScanFun rowScanFun,
            const std::vector<HashScan::RowMaterializeFun>& rowMaterializeFuns, uint32_t numConjuncts);

    /**
     * @brief Scans over all entries in the hash table
     *
     * Processes all valid entries with the associated scan queries.
     */
    void process();

private:
    Table& mTable;
    uint64_t mMinVersion;

    size_t mStart;
    size_t mEnd;
};

/**
 * @brief Garbage collector for the Log-Structured Memory approach scanning over the hash table
 */
class HashScanGarbageCollector {
public:
    HashScanGarbageCollector(LogstructuredMemoryStore&)
    {}

    void run(const std::vector<Table*>& tables, uint64_t minVersion);
};

} // namespace logstructured
} // namespace store
} // namespace tell
