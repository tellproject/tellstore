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
#include <util/Log.hpp>
#include <util/ScanQuery.hpp>

#include <crossbow/allocator.hpp>
#include <crossbow/non_copyable.hpp>

#include <cstdint>
#include <vector>

namespace tell {
namespace store {

struct LogstructuredMemoryStore;

namespace logstructured {

class ChainedVersionRecord;
class GcScanGarbageCollector;
class GcScanProcessor;
class Table;

class GcScan : public LLVMRowScanBase {
public:
    using ScanProcessor = GcScanProcessor;
    using GarbageCollector = GcScanGarbageCollector;

    GcScan(Table* table, std::vector<ScanQuery*> queries);

    /**
     * @brief Start a full scan of this table
     *
     * @param numThreads Number of threads to use for the scan
     * @return A scan processor for each thread
     */
    std::vector<std::unique_ptr<GcScanProcessor>> startScan(size_t numThreads);

private:
    Table* mTable;

    crossbow::allocator mAllocator;
};

/**
 * @brief Scan processor for the Log-Structured Memory approach that performs Garbage Collection as part of its scan
 */
class GcScanProcessor : public LLVMRowScanProcessorBase {
public:
    using LogImpl = Log<UnorderedLogImpl>;
    using PageIterator = LogImpl::PageIterator;

    GcScanProcessor(Table& table, const std::vector<ScanQuery*>& queries, const PageIterator& begin,
            const PageIterator& end, uint64_t minVersion, GcScan::RowScanFun rowScanFun,
            const std::vector<GcScan::RowMaterializeFun>& rowMaterializeFuns, uint32_t numConjuncts);

    /**
     * @brief Scans over all entries in the log
     *
     * Processes all valid entries with the associated scan queries.
     *
     * Performs garbage collection while scanning over a page.
     */
    void process();

private:
    /**
     * @brief Advance the entry iterator to the next entry, advancing to the next page if necessary
     *
     * The processor must not be at the end when calling this function.
     *
     * @return True if the iterator points to a valid element, false if it reached the end
     */
    bool advanceEntry();

    /**
     * @brief Advance the page iterator to the next page containing a valid entry
     *
     * The processor must be at the end of a page but not at the end of the log when calling this function.
     *
     * @return True if the iterator points to a valid element, false if it reached the end
     */
    bool advancePage();

    /**
     * @brief Recycle the given element
     *
     * Copies the element to a recycling page and replaces the old element in the version list with the new element.
     *
     * @param oldElement The element to recycle
     * @param size Size of the old element
     * @param type Type of the old element
     */
    void recycleEntry(ChainedVersionRecord* oldElement, uint32_t size, uint32_t type);

    /**
     * @brief Replaces the given old element with the given new element in the version list of the record
     *
     * The two elements must belong to the same record.
     *
     * @param oldElement Old element to remove from the version list
     * @param newElement New element to replace the old element with
     * @return Whether the replacement was successful
     */
    bool replaceElement(ChainedVersionRecord* oldElement, ChainedVersionRecord* newElement);

    Table& mTable;
    uint64_t mMinVersion;

    LogImpl::PageIterator mPagePrev;
    LogImpl::PageIterator mPageIt;
    LogImpl::PageIterator mPageEnd;

    LogPage::EntryIterator mEntryIt;
    LogPage::EntryIterator mEntryEnd;

    LogPage* mRecyclingHead;
    LogPage* mRecyclingTail;

    /// Amount of garbage in the current page
    uint32_t mGarbage;

    /// Whether all entries in the current page were sealed
    bool mSealed;

    /// Whether the current page is being recycled
    /// Initialized to false to prevent the first page from being garbage collected
    bool mRecycle;
};

/**
 * @brief Garbage collector for the Log-Structured Memory approach that performs Garbage Collection as part of its scan
 */
class GcScanGarbageCollector {
public:
    GcScanGarbageCollector(LogstructuredMemoryStore& storage)
            : mStorage(storage) {
    }

    void run(const std::vector<Table*>& tables, uint64_t minVersion);

private:
    LogstructuredMemoryStore& mStorage;
};

} // namespace logstructured
} // namespace store
} // namespace tell
