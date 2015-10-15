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

#include "InsertHash.hpp"
#include "Record.hpp"
#include "rowstore/RowStorePage.hpp"
#include "rowstore/RowStoreRecord.hpp"
#include "rowstore/RowStoreScanProcessor.hpp"

#include <util/CuckooHash.hpp>
#include <util/Log.hpp>

#include <tellstore/Record.hpp>

#include <crossbow/allocator.hpp>

#include <memory>
#include <vector>
#include <limits>
#include <atomic>
#include <functional>

namespace tell {
namespace commitmanager {
class SnapshotDescriptor;
} // namespace commitmanager

namespace store {

class PageManager;
class ScanQuery;

namespace deltamain {

class Table {
public:
#if defined USE_ROW_STORE
    using ScanProcessor = RowStoreScanProcessor;
    using Page = RowStoreMainPage;
    using PageModifier = RowStorePageModifier;

    using MainRecord = RowStoreRecord;
    using ConstMainRecord = ConstRowStoreRecord;
#else
#error "Unknown storage layout"
#endif

    Table(PageManager& pageManager, const Schema& schema, uint64_t idx, uint64_t insertTableCapacity);

    ~Table();

    const Record& record() const {
        return mRecord;
    }

    const Schema& schema() const {
        return mRecord.schema();
    }

    TableType type() const {
        return mRecord.schema().type();
    }

    int get(uint64_t key, size_t& size, const char*& data, const commitmanager::SnapshotDescriptor& snapshot,
            uint64_t& version, bool& isNewest) const;

    int insert(uint64_t key, size_t size, const char* data, const commitmanager::SnapshotDescriptor& snapshot);

    int update(uint64_t key, size_t size, const char* data, const commitmanager::SnapshotDescriptor& snapshot);

    int remove(uint64_t key, const commitmanager::SnapshotDescriptor& snapshot);

    int revert(uint64_t key, const commitmanager::SnapshotDescriptor& snapshot);

    void runGC(uint64_t minVersion);

    /**
     * prepares a shared scan executed in parallel for the given number
     * of threads, the queryBuffer and the queries themselves. Returns one
     * ScanProcessor object per thread that encapsulates all relevant information
     * to perform the scan (using ScanProcessor.process()). The method assigns
     * each thread the same amount (storage) pages and the last thread gets the
     * insert log in addition.
     */
    std::vector<ScanProcessor> startScan(size_t numThreads, const char* queryBuffer,
            const std::vector<ScanQuery*>& queries) const;

private:
    struct PageList {
        PageList() = default;

        PageList(Log<OrderedLogImpl>::LogIterator insert, Log<OrderedLogImpl>::LogIterator update)
                : insertEnd(insert),
                  updateEnd(update) {
        }

        /// List of pages in the main
        std::vector<Page*> pages;

        /// Iterator pointing to the first element in the insert log not contained in the main pages
        Log<OrderedLogImpl>::LogIterator insertEnd;

        /// Iterator pointing to the first element in the update log not contained in the main pages
        Log<OrderedLogImpl>::LogIterator updateEnd;
    };

    int genericUpdate(uint64_t key, size_t size, const char* data, const commitmanager::SnapshotDescriptor& snapshot,
            RecordType type);

    template <typename Rec>
    bool internalGet(const void* ptr, size_t& size, const char*& data, uint64_t& version, bool& isNewest,
            const commitmanager::SnapshotDescriptor& snapshot, int& ec) const;

    template <typename Rec>
    bool internalUpdate(void* ptr, size_t size, const char* data, const commitmanager::SnapshotDescriptor& snapshot,
            RecordType expectedType, RecordType newType, int& ec);

    template <typename Rec>
    int canUpdate(const Rec& record, const commitmanager::SnapshotDescriptor& snapshot, RecordType expectedType);

    template <typename Rec>
    bool internalRevert(void* ptr, const commitmanager::SnapshotDescriptor& snapshot, int& ec);

    PageManager& mPageManager;
    Record mRecord;
    InsertLogTable mInsertTable;
    Log<OrderedLogImpl> mInsertLog;
    Log<OrderedLogImpl> mUpdateLog;
    std::atomic<CuckooTable*> mMainTable;
    std::atomic<PageList*> mPages;
};

class GarbageCollector {
public:
    void run(const std::vector<Table*>& tables, uint64_t minVersion);
};

} // namespace deltamain
} // namespace store
} // namespace tell
