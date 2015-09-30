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
#include "Record.hpp"
#include "InsertMap.hpp"

#include <config.h>
#include <commitmanager/SnapshotDescriptor.hpp>

#include <memory>

namespace tell {
namespace store {
namespace deltamain {
namespace {

/**
 * @brief Expected number of elements per page
 *
 * This is just a very rough heuristic, but should hopefully be good enough
 */
uint32_t pageCapacity(const Record& record) {
    return TELL_PAGE_SIZE / (28 + record.minimumSize() + record.varSizeFieldCount() * (8 + 4 * VAR_SIZED_OVERHEAD)) + 1;
}

} // anonymous namespace

Table::Table(PageManager& pageManager, const Schema& schema, uint64_t /* idx */)
    : mPageManager(pageManager)
    , mRecord(std::move(schema))
    , mHashTable(crossbow::allocator::construct<CuckooTable>(pageManager))
    , mInsertLog(pageManager)
    , mUpdateLog(pageManager)
    , mPages(crossbow::allocator::construct<PageList>(mInsertLog.begin()))
    , mNumberOfFixedSizedFields(mRecord.fixedSizeFieldCount())
    , mNumberOfVarSizedFields(mRecord.varSizeFieldCount())
    , mPageCapacity(pageCapacity(mRecord))
{}

Table::~Table() {
    crossbow::allocator::destroy_now(mPages.load());

    auto ht = mHashTable.load();
    ht->destroy();
    crossbow::allocator::destroy_now(ht);
}

bool Table::get(uint64_t key,
                size_t& size,
                const char*& data,
                const commitmanager::SnapshotDescriptor& snapshot,
                uint64_t& version,
                bool& isNewest) const {
    auto ptr = mHashTable.load()->get(key);
    if (ptr) {
        CDMRecord rec(reinterpret_cast<char*>(ptr));
        bool wasDeleted;
        bool isValid;
        //TODO: in the column-map case, it might be benefitial to first ucall
        //data(.) with copyData set to false and only if !wasDeleted, call it
        //again with copyData enabled.
        data = rec.data(snapshot, size, version, isNewest, isValid, &wasDeleted);
        // if the newest version is a delete, it might be that there is
        // a new insert in the insert log
        if (isValid && !(wasDeleted && isNewest)) {
            return (data != nullptr && !wasDeleted);
        }
    }
    // in this case we need to scan through the insert log
    auto iter = mInsertLog.begin();
    auto iterEnd = mInsertLog.end();
    for (; iter != iterEnd; ++iter) {
        if (!iter->sealed()) continue;
        CDMRecord rec(iter->data());
        if (rec.isValidDataRecord() && rec.key() == key) {
            bool wasDeleted;
            bool isValid;
            data = rec.data(snapshot, size, version, isNewest, isValid, &wasDeleted);
            if (isNewest && wasDeleted) {
                // same as above, it could be that the record was inserted and
                // then updated - in this case we to continue scanning
                continue;
            }
            return (data != nullptr && !wasDeleted);
        }
    }
    // in this case the tuple does not exist
    isNewest = true;
    size = 0u;
    data = nullptr;
    return false;
}

void Table::insert(uint64_t key,
                   size_t size,
                   const char* const data,
                   const commitmanager::SnapshotDescriptor& snapshot,
                   bool* succeeded /*= nullptr*/) {
    // we need to get the iterator as a first step to make
    // sure to check the part of the log that was visible
    // at this point in time
    auto iter = mInsertLog.begin();
    auto ptr = mHashTable.load()->get(key);
    if (ptr) {
        // the key exists... but it could be, that it got deleted
        CDMRecord rec(reinterpret_cast<const char*>(ptr));
        bool wasDeleted, isNewest;
        bool isValid;

        uint64_t version;
        size_t s;
        rec.data(snapshot, s, version, isNewest, isValid, &wasDeleted, this, false);

        if (isValid && !(wasDeleted && isNewest)) {
            if (succeeded) *succeeded = false;
            return;
        }
        // the tuple was deleted/reverted and we don't have a
        // write-write conflict, therefore we can continue
        // with the insert
    }
    // To do an insert, we optimistically append it to the log.
    // Then we check for conflicts iff the user wants to know whether
    // the insert succeeded.
    auto logEntrySize = size + DMRecord::spaceOverhead(DMRecord::Type::LOG_INSERT);
    auto entry = mInsertLog.append(logEntrySize);
    // We do this in another scope, after this scope is closed, the log
    // is read only (when seal is called)
    auto iterEnd = mInsertLog.end();
    DMRecord insertRecord(entry->data());
    insertRecord.setType(DMRecord::Type::LOG_INSERT);
    insertRecord.writeKey(key);
    insertRecord.writeVersion(snapshot.version());
    insertRecord.writePrevious(nullptr);
    insertRecord.writeData(size, data);
    for (; iter != iterEnd; ++iter) {
        // we busy wait if the entry was not sealed
        while (iter->data() != entry->data() && !iter->sealed()) {}
        const LogEntry* en = iter.operator->();
        if (en == entry) {
            entry->seal();
            if (succeeded) *succeeded = true;
            return;
        }
        CDMRecord rec(en->data());
        if (rec.isValidDataRecord() && rec.key() == key) {
            insertRecord.revert(snapshot.version());
            entry->seal();
            if (succeeded) *succeeded = false;
            return;
        }
    }
    LOG_ASSERT(false, "We should never reach this point");
}

bool Table::update(uint64_t key,
                   size_t size,
                   const char* const data,
                   const commitmanager::SnapshotDescriptor& snapshot)
{
    auto fun = [this, key, size, data, &snapshot]()
    {
        auto logEntrySize = size + DMRecord::spaceOverhead(DMRecord::Type::LOG_UPDATE);
        auto entry = mUpdateLog.append(logEntrySize);
        {
            DMRecord updateRecord(entry->data());
            updateRecord.setType(DMRecord::Type::LOG_UPDATE);
            updateRecord.writeKey(key);
            updateRecord.writeVersion(snapshot.version());
            updateRecord.writePrevious(nullptr);
            updateRecord.writeData(size, data);
        }
        return entry->data();
    };
    return genericUpdate(fun, key, snapshot);
}

bool Table::remove(uint64_t key, const commitmanager::SnapshotDescriptor& snapshot) {
    auto fun = [this, key, &snapshot]() {
        auto logEntrySize = DMRecord::spaceOverhead(DMRecord::Type::LOG_DELETE);
        auto entry = mUpdateLog.append(logEntrySize);
        DMRecord rmRecord(entry->data());
        rmRecord.setType(DMRecord::Type::LOG_DELETE);
        rmRecord.writeKey(key);
        rmRecord.writeVersion(snapshot.version());
        rmRecord.writePrevious(nullptr);
        return entry->data();
    };
    return genericUpdate(fun, key, snapshot);
}

bool Table::revert(uint64_t key, const commitmanager::SnapshotDescriptor& snapshot) {
    // TODO Implement
    return false;
}

template<class Fun>
bool Table::genericUpdate(const Fun& appendFun,
                          uint64_t key,
                          const commitmanager::SnapshotDescriptor& snapshot)
{
    auto iter = mInsertLog.begin();
    auto iterEnd = mInsertLog.end();
    auto ptr = mHashTable.load()->get(key);
    if (!ptr) {
        for (; iter != iterEnd; ++iter) {
            CDMRecord rec(iter->data());
            if (rec.isValidDataRecord() && rec.key() == key) {
                // we found it!
                ptr = iter->data();
                break;
            }
        }
    }
    if (!ptr) {
        // no record with key exists
        return false;
    }
    // now we found it. Therefore we first append the
    // update optimistically
    char* nextPtr = appendFun();
    DMRecord rec(reinterpret_cast<char*>(ptr));
    bool isValid;
    return rec.update(nextPtr, isValid, snapshot, this);
}

std::vector<Table::ScanProcessor> Table::startScan(size_t numThreads, const char* queryBuffer,
        const std::vector<ScanQuery*>& queries) const
{
    auto alloc = std::make_shared<crossbow::allocator>();
    auto pageList = mPages.load();
    decltype(mInsertLog.end()) insIter(pageList->insertBegin.page(), pageList->insertBegin.offset());
    auto endIns = mInsertLog.end();
    auto numPages = pageList->pages.size();
    std::vector<ScanProcessor> result;
    result.reserve(numThreads);
    size_t beginIdx = 0;
    auto mod = numPages % numThreads;
    for (decltype(numThreads) i = 0; i < numThreads; ++i) {
        const auto& startIter = (i == numThreads - 1 ? insIter : endIns);
        auto endIdx = beginIdx + numPages / numThreads + (i < mod ? 1 : 0);
        result.emplace_back(alloc, pageList->pages, beginIdx, endIdx, startIter, endIns, &mPageManager, queryBuffer,
                queries, &mRecord);
        beginIdx = endIdx;
    }
    return result;
}

void Table::runGC(uint64_t minVersion) {
    crossbow::allocator _;
    auto hashTable = mHashTable.load()->modifier();

    auto pageList = mPages.load();

    // We need to process the insert-log first. There might be deleted records which have an insert
    // Get the iterators for the update log first to not loose any updates when truncating
    auto updateBegin = mUpdateLog.end();
    auto updateEnd = mUpdateLog.end();
    auto insBegin = pageList->insertBegin;
    auto insIter = insBegin;
    auto insEnd = mInsertLog.end();
    InsertMap insertMap;
    for (; insIter != insEnd && insIter->sealed(); ++insIter) {
        CDMRecord rec(insIter->data());
        if (!rec.isValidDataRecord()) continue;
        auto k = rec.key();
        insertMap[InsertMapKey(k)].push_back(insIter->data());
    }
    auto newPageList = crossbow::allocator::construct<PageList>(insEnd);
    auto& nPages = newPageList->pages;
    // this loop just iterates over all pages
    Page page(mPageManager, nullptr, this);
    for (auto roPage: pageList->pages) {
        page.reset(roPage);
        bool done;
        auto newPage = page.gc(minVersion, insertMap, done, hashTable);
        while (!done) {
            LOG_ASSERT(newPage != nullptr, "newPage should not be NULL when done is false!")
            nPages.push_back(newPage);
            newPage = page.gc(minVersion, insertMap, done, hashTable);
        }
        if (newPage) {
            nPages.push_back(newPage);
        }
    }

    // now we can process the inserts
    while (!insertMap.empty()) {
        nPages.push_back(page.fillWithInserts(minVersion, insertMap, hashTable));
    }

    // The garbage collection is finished - we can now reset the read only table
    mPages.store(newPageList);
    crossbow::allocator::destroy(pageList);
    {
        auto ht = mHashTable.load();
        mHashTable.store(hashTable.done());
        crossbow::allocator::destroy(ht);
    }
    __attribute__((unused)) auto insertRes = mInsertLog.truncateLog(insBegin, insIter);
    LOG_ASSERT(insertRes, "Truncating insert log did not succeed");
    __attribute__((unused)) auto updateRes = mUpdateLog.truncateLog(updateBegin, updateEnd);
    LOG_ASSERT(updateRes, "Truncating update log did not succeed");
}

void GarbageCollector::run(const std::vector<Table*>& tables, uint64_t minVersion) {
    for (auto table : tables) {
        if (table->type() == TableType::NON_TRANSACTIONAL) {
            table->runGC(std::numeric_limits<uint64_t>::max());
        } else {
            table->runGC(minVersion);
        }
    }
}

} // namespace deltamain
} // namespace store
} // namespace tell

