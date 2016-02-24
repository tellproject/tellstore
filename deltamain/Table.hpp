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
#include "colstore/ColumnMapContext.hpp"
#include "colstore/ColumnMapRecord.hpp"
#include "rowstore/RowStoreContext.hpp"

#include <util/Allocator.hpp>
#include <util/CuckooHash.hpp>
#include <util/Log.hpp>

#include <tellstore/ErrorCode.hpp>
#include <tellstore/Record.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/logger.hpp>

#include <atomic>
#include <cstdlib>
#include <functional>
#include <memory>
#include <vector>

namespace tell {
namespace commitmanager {
class SnapshotDescriptor;
} // namespace commitmanager

namespace store {

class PageManager;
class ScanQuery;

namespace deltamain {

template <typename Context>
class Table {
public:
    using Scan = typename Context::Scan;
    using ScanProcessor = typename Scan::ScanProcessor;
    using Page = typename Context::Page;
    using PageModifier = typename Context::PageModifier;

    using MainRecord = typename Context::MainRecord;
    using ConstMainRecord = typename Context::ConstMainRecord;

    static void* operator new(size_t size) {
        LOG_ASSERT(size % alignof(Table) == 0u, "Size must be multiple of alignment");
        return ::aligned_alloc(alignof(Table), size);
    }

    static void operator delete(void* ptr) {
        ::free(ptr);
    }

    Table(MemoryReclaimer& memoryManager, PageManager& pageManager, const crossbow::string& name, const Schema& schema,
            uint64_t idx, uint64_t insertTableCapacity);

    ~Table();

    const crossbow::string& tableName() const {
        return mTableName;
    }

    const Record& record() const {
        return mRecord;
    }

    const Schema& schema() const {
        return mRecord.schema();
    }

    uint64_t tableId() const {
        return mTableId;
    }

    const Context& context() const {
        return mContext;
    }

    TableType type() const {
        return mRecord.schema().type();
    }

    template <typename Fun>
    int get(uint64_t key, const commitmanager::SnapshotDescriptor& snapshot, Fun fun) const;

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
    template <typename... Args>
    std::vector<std::unique_ptr<ScanProcessor>> startScan(size_t numThreads, const std::vector<ScanQuery*>& queries,
            Args&&... args) const;

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

    const InsertLogEntry* getFromInsert(uint64_t key, DynamicInsertTableEntry** headList = nullptr) const;

    InsertLogEntry* getFromInsert(uint64_t key, DynamicInsertTableEntry** headList = nullptr) {
        return const_cast<InsertLogEntry*>(const_cast<const Table<Context>*>(this)->getFromInsert(key, headList));
    }

    int genericUpdate(uint64_t key, size_t size, const char* data, const commitmanager::SnapshotDescriptor& snapshot,
            RecordType type);

    template <typename Rec, typename Fun>
    bool internalGet(const void* ptr, const commitmanager::SnapshotDescriptor& snapshot, Fun fun, int& ec) const;

    template <typename Rec>
    bool internalUpdate(void* ptr, size_t size, const char* data, const commitmanager::SnapshotDescriptor& snapshot,
            RecordType expectedType, RecordType newType, int& ec);

    template <typename Rec>
    int canUpdate(const Rec& record, const commitmanager::SnapshotDescriptor& snapshot, RecordType expectedType);

    template <typename Rec>
    bool internalRevert(void* ptr, const commitmanager::SnapshotDescriptor& snapshot, int& ec);

    MemoryReclaimer& mMemoryManager;
    PageManager& mPageManager;

    crossbow::string mTableName;
    Record mRecord;
    uint64_t mTableId;

    Context mContext;

    alignas(64) Log<OrderedLogImpl> mInsertLog;
    alignas(64) Log<OrderedLogImpl> mUpdateLog;

    alignas(64) DynamicInsertTable mInsertTable;
    std::atomic<CuckooTable*> mMainTable;
    std::atomic<PageList*> mPages;

};

template <typename Context>
template <typename Fun>
int Table<Context>::get(uint64_t key, const commitmanager::SnapshotDescriptor& snapshot, Fun fun) const {
    int ec;

    // Check main first
    auto mainTable = mMainTable.load();
    if (auto ptr = mainTable->get(key)) {
        if (internalGet<ConstMainRecord>(ptr, snapshot, fun, ec)) {
            return ec;
        }
    }

    // Lookup in the insert hash table
    if (auto ptr = getFromInsert(key)) {
        if (internalGet<ConstInsertRecord>(ptr, snapshot, fun, ec)) {
            return ec;
        }
    }

    // Check if the hash table pointer changed
    // This is required as a concurrent running garbage collection might have transferred inserts from the insert log
    // into the (new) main.
    auto newMainTable = mMainTable.load();
    if (newMainTable != mainTable) {
        // Lookup in the new hash table
        if (auto ptr = newMainTable->get(key)) {
            if (internalGet<ConstMainRecord>(ptr, snapshot, fun, ec)) {
                return ec;
            }
        }
    }

    // The element was really not found
    return error::not_found;
}

template <typename Context>
template <typename... Args>
std::vector<std::unique_ptr<typename Table<Context>::ScanProcessor>> Table<Context>::Table::startScan(size_t numThreads,
        const std::vector<ScanQuery*>& queries, Args&&... args) const {
    auto pageList = mPages.load();
    auto insEnd = mInsertLog.end();
    // TODO Make LogIterator convertible to ConstLogIterator
    decltype(insEnd) insIter(pageList->insertEnd.page(), pageList->insertEnd.offset());
    auto numPages = pageList->pages.size();
    std::vector<std::unique_ptr<ScanProcessor>> result;
    result.reserve(numThreads);
    size_t beginIdx = 0;
    auto mod = numPages % numThreads;
    for (decltype(numThreads) i = 0; i < numThreads; ++i) {
        const auto& startIter = (i == numThreads - 1 ? insIter : insEnd);
        auto endIdx = beginIdx + numPages / numThreads + (i < mod ? 1 : 0);
        result.emplace_back(new ScanProcessor(mContext, mRecord, queries, pageList->pages, beginIdx, endIdx, startIter,
                insEnd, std::forward<Args>(args)...));
        beginIdx = endIdx;
    }
    return result;
}

template <typename Context>
template <typename Rec, typename Fun>
bool Table<Context>::internalGet(const void* ptr, const commitmanager::SnapshotDescriptor& snapshot, Fun fun, int& ec)
        const {
    Rec record(ptr, mContext);
    if (!record.valid()) {
        return false;
    }

    // Follow the recycled record in case the current record was garbage collected in the meantime
    if (auto main = newestMainRecord(record.newest())) {
        return internalGet<ConstMainRecord>(main, snapshot, std::move(fun), ec);
    }

    // Lookup in update history
    bool isNewest = true;
    UpdateRecordIterator updateIter(reinterpret_cast<const UpdateLogEntry*>(record.newest()), record.baseVersion());
    for (; !updateIter.done(); updateIter.next()) {
        if (!snapshot.inReadSet(updateIter->version)) {
            isNewest = false;
            continue;
        }

        // Check if the entry marks a deletion: Return element not found
        auto entry = LogEntry::entryFromData(reinterpret_cast<const char*>(updateIter.value()));
        if (entry->type() == crossbow::to_underlying(RecordType::DELETE)) {
            ec = (isNewest ? error::not_found : error::not_in_snapshot);
            return true;
        }

        auto size = entry->size() - sizeof(UpdateLogEntry);
        auto dest = fun(size, updateIter->version, isNewest);
        memcpy(dest, updateIter->data(), size);

        ec = 0;
        return true;
    }

    // Lookup in base
    ec = record.get(updateIter.lowestVersion(), snapshot, std::move(fun), isNewest);
    return true;
}

template <typename Context>
class GarbageCollector {
public:
    void run(const std::vector<Table<Context>*>& tables, uint64_t minVersion);
};

extern template class Table<RowStoreContext>;
extern template class GarbageCollector<RowStoreContext>;

extern template class Table<ColumnMapContext>;
extern template class GarbageCollector<ColumnMapContext>;

} // namespace deltamain
} // namespace store
} // namespace tell
