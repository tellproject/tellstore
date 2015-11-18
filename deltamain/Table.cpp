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

#include "Table.hpp"

#include <config.h>

#include <boost/config.hpp>

namespace tell {
namespace store {
namespace deltamain {

template <typename Context>
Table<Context>::Table(PageManager& pageManager, const Schema& schema, uint64_t /* idx */, uint64_t insertTableCapacity)
    : mPageManager(pageManager)
    , mRecord(std::move(schema))
    , mInsertTable(insertTableCapacity)
    , mInsertLog(pageManager)
    , mUpdateLog(pageManager)
    , mMainTable(crossbow::allocator::construct<CuckooTable>(pageManager))
    , mPages(crossbow::allocator::construct<PageList>(mInsertLog.begin(), mUpdateLog.begin()))
    , mContext(mPageManager, mRecord)
{}

template <typename Context>
Table<Context>::~Table() {
    auto pageList = mPages.load();
    for (auto page : pageList->pages) {
        mPageManager.free(page);
    }
    crossbow::allocator::destroy_now(pageList);

    auto ht = mMainTable.load();
    ht->destroy();
    crossbow::allocator::destroy_now(ht);
}

template <typename Context>
int Table<Context>::insert(uint64_t key, size_t size, const char* data,
        const commitmanager::SnapshotDescriptor& snapshot) {
    int ec;

    // Check main
    auto mainTable = mMainTable.load();
    if (auto ptr = mainTable->get(key)) {
        if (internalUpdate<MainRecord>(ptr, size, data, snapshot, RecordType::DELETE, RecordType::DATA, ec)) {
            return ec;
        }
    }

    // Check insert log
    DynamicInsertTableEntry* insertList;
    if (auto ptr = getFromInsert(key, &insertList)) {
        if (internalUpdate<InsertRecord>(ptr, size, data, snapshot, RecordType::DELETE, RecordType::DATA, ec)) {
            return ec;
        }

        // Try to remove the invalid insert record from the hash table and retry from the beginning
        mInsertTable.remove(key, ptr, insertList);
    }

    // Write into insert log
    auto logEntry = mInsertLog.append(size + sizeof(InsertLogEntry));
    if (!logEntry) {
        LOG_FATAL("Failed to append to log");
        return error::out_of_memory;
    }
    auto insertEntry = new (logEntry->data()) InsertLogEntry(key, snapshot.version());
    memcpy(insertEntry->data(), data, size);

    // Try to insert the element in the insert table
    // If the element changed it could be invalidate in the meantime
    if (!mInsertTable.insert(key, insertEntry, insertList)) {
        insertEntry->newest.store(crossbow::to_underlying(NewestPointerTag::INVALID));
        mInsertLog.seal(logEntry);
        return error::not_in_snapshot;
    }

    // Check if the main has changed in the meantime
    auto newMainTable = mMainTable.load();
    if (newMainTable != mainTable) {
        // The pointer can never point to the record being currently written as the garbage collection waits for all log
        // entries to be sealed. If the main record is invalid the insert has succeeded because it was already written
        // into the hash table.
        if (auto ptr = newMainTable->get(key)) {
            if (internalUpdate<MainRecord>(ptr, size, data, snapshot, RecordType::DELETE, RecordType::DATA, ec)) {
                insertEntry->newest.store(crossbow::to_underlying(NewestPointerTag::INVALID));
                mInsertLog.seal(logEntry);
                mInsertTable.remove(key, insertEntry, insertList);
                return ec;
            }
        }
    }

    mInsertLog.seal(logEntry);
    return 0;
}

template <typename Context>
int Table<Context>::update(uint64_t key, size_t size, const char* data,
        const commitmanager::SnapshotDescriptor& snapshot) {
    return genericUpdate(key, size, data, snapshot, RecordType::DATA);
}

template <typename Context>
int Table<Context>::remove(uint64_t key, const commitmanager::SnapshotDescriptor& snapshot) {
    return genericUpdate(key, 0, nullptr, snapshot, RecordType::DELETE);
}

template <typename Context>
int Table<Context>::revert(uint64_t key, const commitmanager::SnapshotDescriptor& snapshot) {
    int ec;

    // Check main
    auto mainTable = mMainTable.load();
    if (auto ptr = mainTable->get(key)) {
        if (internalRevert<MainRecord>(ptr, snapshot, ec)) {
            return ec;
        }
    }

    // Lookup in the insert hash table
    if (auto ptr = getFromInsert(key)) {
        if (internalRevert<InsertRecord>(ptr, snapshot, ec)) {
            return ec;
        }
    }

    // Check if the hash table pointer changed
    // This is required as a concurrent running garbage collection might have transferred inserts from the insert log
    // into the (new) main.
    auto newMainTable = mMainTable.load();
    if (newMainTable != mainTable) {
        if (auto ptr = newMainTable->get(key)) {
            if (internalRevert<MainRecord>(ptr, snapshot, ec)) {
                return ec;
            }
        }
    }

    // Element not found
    return 0;
}

template <typename Context>
int Table<Context>::genericUpdate(uint64_t key, size_t size, const char* data,
        const commitmanager::SnapshotDescriptor& snapshot, RecordType newType) {
    int ec;

    // Check main
    auto mainTable = mMainTable.load();
    if (auto ptr = mainTable->get(key)) {
        if (internalUpdate<MainRecord>(ptr, size, data, snapshot, RecordType::DATA, newType, ec)) {
            return ec;
        }
    }

    // Lookup in the insert hash table
    if (auto ptr = getFromInsert(key)) {
        if (internalUpdate<InsertRecord>(ptr, size, data, snapshot, RecordType::DATA, newType, ec)) {
            return ec;
        }
    }

    // Check if the hash table pointer changed
    // This is required as a concurrent running garbage collection might have transferred inserts from the insert log
    // into the (new) main.
    auto newMainTable = mMainTable.load();
    if (newMainTable != mainTable) {
        if (auto ptr = newMainTable->get(key)) {
            if (internalUpdate<MainRecord>(ptr, size, data, snapshot, RecordType::DATA, newType, ec)) {
                return ec;
            }
        }
    }

    // The element was really not found
    return error::invalid_write;
}

template <typename Context>
const InsertLogEntry* Table<Context>::getFromInsert(uint64_t key, DynamicInsertTableEntry** headList) const {
    auto ptr = mInsertTable.get(key, headList);
    if (!ptr) {
        return nullptr;
    }

    auto logEntry = LogEntry::entryFromData(reinterpret_cast<const char*>(ptr));
    if (BOOST_UNLIKELY(!logEntry->sealed())) {
        return nullptr;
    }

    return reinterpret_cast<const InsertLogEntry*>(ptr);
}

template <typename Context>
void Table<Context>::runGC(uint64_t minVersion) {
    LOG_TRACE("Starting garbage collection [minVersion = %1%]", minVersion);

    crossbow::allocator _;
    auto oldMainTable = mMainTable.load();
    auto mainTableModifier = oldMainTable->modifier();

    PageModifier pageListModifier(mContext, mPageManager, mainTableModifier, minVersion);

    auto pageList = crossbow::allocator::construct<PageList>();
    pageList->updateEnd = mUpdateLog.sealedEnd();

    std::vector<void*> obsoletePages;
    auto oldPageList = mPages.load();
    for (auto oldPage: oldPageList->pages) {
        if (pageListModifier.clean(oldPage)) {
            obsoletePages.emplace_back(oldPage);
        }
    }

    // Allocate a new insert hash table head
    auto insertHeadList = mInsertTable.allocateHead();

    auto insBegin = oldPageList->insertEnd;
    auto insEnd = mInsertLog.end();

    for (auto insIter = insBegin; insIter != insEnd; ++insIter) {
        // Busy wait until the entry is sealed
        while (!insIter->sealed());

        InsertRecord insertRecord(reinterpret_cast<InsertLogEntry*>(insIter->data()));
        if (!insertRecord.valid()) {
            continue;
        }

        if (!pageListModifier.append(insertRecord)) {
            mInsertTable.remove(insertRecord.key(), insertRecord.value(), insertHeadList);
        }
    }
    pageList->pages = pageListModifier.done();

    // The garbage collection is finished - we can now reset the read only table
    __attribute__((unused)) auto insertRes = mInsertLog.truncateLog(insBegin, insEnd);
    LOG_ASSERT(insertRes, "Truncating insert log did not succeed");
    __attribute__((unused)) auto updateRes = mUpdateLog.truncateLog(mUpdateLog.begin(), oldPageList->updateEnd);
    LOG_ASSERT(updateRes, "Truncating update log did not succeed");

    pageList->insertEnd = insEnd;

    mMainTable.store(mainTableModifier.done());
    crossbow::allocator::destroy(oldMainTable);

    mPages.store(pageList);
    crossbow::allocator::destroy(oldPageList);

    // Free all obsolete pages from the old main using the epoch mechanism
    auto& pageManager = mPageManager;
    crossbow::allocator::invoke([obsoletePages, &pageManager]() {
        for (auto page : obsoletePages) {
            pageManager.free(page);
        }
    });

    // Truncate the insert hash table and free all tables using the epoch mechanism
    mInsertTable.truncate(insertHeadList);

    LOG_TRACE("Completing garbage collection");
}

template <typename Context>
template <typename Rec>
bool Table<Context>::internalUpdate(void* ptr, size_t size, const char* data,
        const commitmanager::SnapshotDescriptor& snapshot, RecordType expectedType, RecordType newType, int& ec) {
    Rec record(ptr, mContext);
    if (!record.valid()) {
        return false;
    }

    // Check if the entry was garbage collected: Follow link in case it is
    if (auto main = newestMainRecord(record.newest())) {
        return internalUpdate<MainRecord>(main, size, data, snapshot, expectedType, newType, ec);
    }

    LOG_ASSERT(record.newest() % 8 == crossbow::to_underlying(NewestPointerTag::UPDATE),
            "Newest pointer must point to untagged update record");

    // Check if the entry can be overwritten
    if ((ec = canUpdate(record, snapshot, expectedType)) != 0) {
        return true;
    }

    // Write update
    auto logEntry = mUpdateLog.append(size + sizeof(UpdateLogEntry), newType);
    if (!logEntry) {
        LOG_FATAL("Failed to append to log");
        ec = error::out_of_memory;
        return true;
    }
    auto previous = reinterpret_cast<const UpdateLogEntry*>(record.newest());
    auto updateEntry = new (logEntry->data()) UpdateLogEntry(record.key(), snapshot.version(), previous);
    memcpy(updateEntry->data(), data, size);

    // Try to set the newest pointer of the base record to the newly written UpdateLogEntry
    if (!record.tryUpdate(reinterpret_cast<uintptr_t>(updateEntry))) {
        updateEntry->previous.store(crossbow::to_underlying(NewestPointerTag::INVALID));
        mUpdateLog.seal(logEntry);

        // If the newest pointer points to a main record then the base was garbage collected in the meantime
        // Retry the write again on the new main record.
        if (auto main = newestMainRecord(record.newest())) {
            return internalUpdate<MainRecord>(main, size, data, snapshot, expectedType, newType, ec);
        }

        // Another update happened in the meantime
        ec  = error::not_in_snapshot;
        return true;
    }
    mUpdateLog.seal(logEntry);

    ec = 0;
    return true;
}

template <typename Context>
template <typename Rec>
int Table<Context>::canUpdate(const Rec& record, const commitmanager::SnapshotDescriptor& snapshot,
        RecordType expectedType) {
    UpdateRecordIterator updateIter(reinterpret_cast<const UpdateLogEntry*>(record.newest()), record.baseVersion());
    if (!updateIter.done()) {
        if (!snapshot.inReadSet(updateIter->version)) {
            return error::not_in_snapshot;
        }
        auto entry = LogEntry::entryFromData(reinterpret_cast<const char*>(updateIter.value()));

        // Check if the entry can be written
        return (entry->type() == expectedType ? 0 : error::invalid_write);
    }

    return record.canUpdate(updateIter.lowestVersion(), snapshot, expectedType);
}

template <typename Context>
template <typename Rec>
bool Table<Context>::internalRevert(void* ptr, const commitmanager::SnapshotDescriptor& snapshot, int& ec) {
    Rec record(ptr, mContext);
    if (!record.valid()) {
        return false;
    }

    // Check if the entry was garbage collected: Follow link in case it is
    if (auto main = newestMainRecord(record.newest())) {
        return internalRevert<MainRecord>(main, snapshot, ec);
    }

    LOG_ASSERT(record.newest() % 8 == crossbow::to_underlying(NewestPointerTag::UPDATE),
            "Newest pointer must point to untagged update record");

    UpdateRecordIterator updateIter(reinterpret_cast<const UpdateLogEntry*>(record.newest()), record.baseVersion());
    if (!updateIter.done()) {
        if (updateIter->version < snapshot.version()) {
            ec = 0;
            return true;
        }
        // Check if version history has element
        if (updateIter->version > snapshot.version()) {
            for (; !updateIter.done(); updateIter.next()) {
                if (updateIter->version < snapshot.version()) {
                    ec = 0;
                    return true;
                }
                if (updateIter->version == snapshot.version()) {
                    ec = error::not_in_snapshot;
                    return true;
                }
            }
            ec = 0;
            return true;
        }
    } else {
        bool needsRevert;
        ec = record.canRevert(updateIter.lowestVersion(), snapshot, needsRevert);
        if (!needsRevert) {
            return ec;
        }
    }

    // Write update
    auto logEntry = mUpdateLog.append(sizeof(UpdateLogEntry), RecordType::REVERT);
    if (!logEntry) {
        LOG_FATAL("Failed to append to log");
        ec = error::out_of_memory;
        return true;
    }
    auto previous = reinterpret_cast<const UpdateLogEntry*>(record.newest());
    auto updateEntry = new (logEntry->data()) UpdateLogEntry(record.key(), snapshot.version(), previous);

    // Try to set the newest pointer of the base record to the newly written UpdateLogEntry
    if (!record.tryUpdate(reinterpret_cast<uintptr_t>(updateEntry))) {
        updateEntry->previous.store(crossbow::to_underlying(NewestPointerTag::INVALID));
        mUpdateLog.seal(logEntry);

        // If the newest pointer points to a main record then the base was garbage collected in the meantime
        // Retry the write again on the new main record.
        if (auto main = newestMainRecord(record.newest())) {
            return internalRevert<MainRecord>(main, snapshot, ec);
        }

        // Another update happened in the meantime
        ec = error::not_in_snapshot;
        return true;
    }
    mUpdateLog.seal(logEntry);

    ec = 0;
    return true;
}

template <typename Context>
void GarbageCollector<Context>::run(const std::vector<Table<Context>*>& tables, uint64_t minVersion) {
    for (auto table : tables) {
        if (table->type() == TableType::NON_TRANSACTIONAL) {
            table->runGC(std::numeric_limits<uint64_t>::max() - 1);
        } else {
            table->runGC(minVersion);
        }
    }
}

template class Table<RowStoreContext>;
template class GarbageCollector<RowStoreContext>;

template class Table<ColumnMapContext>;
template class GarbageCollector<ColumnMapContext>;

} // namespace deltamain
} // namespace store
} // namespace tell
