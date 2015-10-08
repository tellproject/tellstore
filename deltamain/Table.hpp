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

#include "rowstore/RowStorePage.hpp"
#include "colstore/ColumnMapPage.hpp"
#include "rowstore/RowStoreScanProcessor.hpp"
#include "colstore/ColumnMapScanProcessor.hpp"

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
    struct PageList {
        PageList(Log<OrderedLogImpl>::LogIterator begin)
                : insertBegin(begin) {
        }

        /// List of pages in the main
        std::vector<char*> pages;

        /// Iterator pointing to the first element not contained in the main pages
        Log<OrderedLogImpl>::LogIterator insertBegin;
    };

    PageManager& mPageManager;
    Record mRecord;
    std::atomic<CuckooTable*> mHashTable;
    Log<OrderedLogImpl> mInsertLog;
    Log<OrderedLogImpl> mUpdateLog;
    std::atomic<PageList*> mPages;
    const uint32_t mNumberOfFixedSizedFields;   //@braunl: added for speedup
    const uint32_t mNumberOfVarSizedFields;     //@braunl: added for speedup
    const uint32_t mPageCapacity;               //@braunl: added for speedup

public:
    Table(PageManager& pageManager, const Schema& schema, uint64_t idx);

    ~Table();

#if defined USE_ROW_STORE
    using ScanProcessor = RowStoreScanProcessor;
    using Page = RowStorePage;
#elif defined USE_COLUMN_MAP
    using ScanProcessor = ColumnMapScanProcessor;
    using Page = ColumnMapPage;
#else
#error "Unknown storage layout"
#endif

    const Record& record() const {
        return mRecord;
    }

    const Schema& schema() const {
        return mRecord.schema();
    }

/**
 * @braunl: added the following helper functions used by the columnMap store
 */
    const PageManager* pageManager() const {
        return pageManager();
    }

    const int32_t getFieldOffset(const Record::id_t id) const {
        return mRecord.getFieldMeta(id).second;
    }

    /**
     * assumes var-sized fields are constant size 8 (offset + prefix)
     */
    const size_t getFieldSize(const Record::id_t id) const {
        return id < mNumberOfFixedSizedFields ? mRecord.schema().fixedSizeFields().at(id).defaultSize() : 8;
    }

    const uint32_t getNumberOfFixedSizedFields() const {
        return mNumberOfFixedSizedFields;
    }

    const uint32_t getNumberOfVarSizedFields() const {
        return mNumberOfVarSizedFields;
    }

    const uint32_t getPageCapacity() const {
        return mPageCapacity;
    }
/**
 * @braunl: end of helper functions
 */

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
     * TODO: question: what happens with the update-log?! Shouldn't that be
     * scanned as well?
     */
    std::vector<ScanProcessor> startScan(size_t numThreads, const char* queryBuffer,
            const std::vector<ScanQuery*>& queries) const;
private:
    template<class Fun>
    int genericUpdate(const Fun& appendFun, uint64_t key, const commitmanager::SnapshotDescriptor& snapshot);
};

//TODO: question: isn't that code that could be shared between different approaches?
//Do we really need a separate garbage collector class for every approach? And is this
//the right place to put it?
class GarbageCollector {
public:
    void run(const std::vector<Table*>& tables, uint64_t minVersion);
};

} // namespace deltamain
} // namespace store
} // namespace tell
