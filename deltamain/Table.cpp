#include "Table.hpp"
#include "Record.hpp"
#include "Page.hpp"
#include "InsertMap.hpp"

#include <memory>

namespace tell {
namespace store {
namespace deltamain {


Table::Iterator::Iterator(
        const std::shared_ptr<allocator>& alloc,
        const PageList* pages,
        size_t pageIdx,
        size_t pageEndIdx,
        const LogIterator& logIter,
        const LogIterator& logEnd,
        PageManager* pageManager,
        const Record* record)
    : mAllocator(alloc)
    , pages(pages)
    , pageIdx(pageIdx)
    , pageEndIdx(pageEndIdx)
    , logIter(logIter)
    , logEnd(logEnd)
    , pageManager(pageManager)
    , record(record)
    , pageIter(Page(*pageManager, (*pages)[pageIdx]).begin())
    , pageEnd (Page(*pageManager, (*pages)[pageIdx]).end())
{
    setCurrentEntry();
}

void Table::Iterator::next()
{
    // This assures that the iterator is invalid when we reached the end
    if (currVersionIter.isValid() && (++currVersionIter).isValid()) {
        return;
    }
    if (logIter != logEnd) {
        ++logIter;
    } else if (pageIter != pageEnd) {
        ++pageIter;
    } else {
        ++pageIdx;
        if (pageIdx >= pageEndIdx)
            return;
        Page p(*pageManager, (*pages)[pageIdx]);
        pageIter = p.begin();
        pageEnd = p.end();
    }
    setCurrentEntry();
}

void Table::Iterator::setCurrentEntry()
{
    while (logIter != logEnd) {
        if (logIter->sealed()) {
            CDMRecord rec(logIter->data());
            if (rec.isValidDataRecord()) {
                currVersionIter = rec.getVersionIterator(record);
                return;
            }
        }
        ++logIter;
    }
    if (pageIdx >= pageEndIdx)
        return;
    while (true) {
        while (pageIter != pageEnd) {
            CDMRecord rec(*pageIter);
            if (rec.isValidDataRecord()) {
                currVersionIter = rec.getVersionIterator(record);
                return;
            }
            ++pageIter;
        }
        ++pageIdx;
        if (pageIdx >= pageEndIdx)
            return;
        Page p(*pageManager, (*pages)[pageIdx]);
        pageIter = p.begin();
        pageEnd = p.end();
    }
}


Table::Table(PageManager& pageManager, const Schema& schema, uint64_t /* idx */)
    : mPageManager(pageManager)
    , mSchema(schema)
    , mRecord(schema)
    , mHashTable(new (allocator::malloc(sizeof(CuckooTable))) CuckooTable(pageManager))
    , mInsertLog(pageManager)
    , mUpdateLog(pageManager)
    , mPages(new (allocator::malloc(sizeof(std::vector<char*>))) std::vector<char*>())
{}

bool Table::get(uint64_t key,
                size_t& size,
                const char*& data,
                const SnapshotDescriptor& snapshot,
                bool& isNewest) const {
    auto ptr = mHashTable.load()->get(key);
    if (ptr) {
        CDMRecord rec(reinterpret_cast<char*>(ptr));
        bool wasDeleted;
        bool isValid;
        data = rec.data(snapshot, size, isNewest, isValid, &wasDeleted);
        // if the newest version is a delete, it might be that there is
        // a new insert in the insert log
        if (isValid && !(wasDeleted && isNewest)) {
            return !wasDeleted;
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
            data = rec.data(snapshot, size, isNewest, isValid, &wasDeleted);
            if (isNewest && wasDeleted) {
                // same as above, it could be that the record was inserted and
                // then updated - in this case we to continue scanning
                continue;
            }
            return !wasDeleted;
        }
    }
    // in this case the tuple does not exist
    return false;
}

bool Table::getNewest(uint64_t key,
                      size_t& size,
                      const char*& data,
                      uint64_t& version) const {
    // TODO Implement
    return false;
}

void Table::insert(uint64_t key,
                   size_t size,
                   const char* const data,
                   const SnapshotDescriptor& snapshot,
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
        size_t s;
        bool isValid;
        rec.data(snapshot, s, isNewest, isValid, &wasDeleted);
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

void Table::insert(uint64_t key,
                   const GenericTuple& tuple,
                   const SnapshotDescriptor& snapshot,
                   bool* succeeded /*= nullptr*/)
{
    size_t size;
    std::unique_ptr<char[]> rec(mRecord.create(tuple, size));
    insert(key, size, rec.get(), snapshot, succeeded);
}

bool Table::update(uint64_t key,
                   size_t size,
                   const char* const data,
                   const SnapshotDescriptor& snapshot)
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

bool Table::remove(uint64_t key, const SnapshotDescriptor& snapshot) {
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

bool Table::revert(uint64_t key, const SnapshotDescriptor& snapshot) {
    // TODO Implement
    return false;
}

template<class Fun>
bool Table::genericUpdate(const Fun& appendFun,
                          uint64_t key,
                          const SnapshotDescriptor& snapshot)
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
    // update optimistaically
    char* nextPtr = appendFun();
    DMRecord rec(reinterpret_cast<char*>(ptr));
    bool isValid;
    return rec.update(nextPtr, isValid, snapshot);
}

std::vector<Table::Iterator> Table::startScan(int numThreads) const
{
    auto alloc = std::make_shared<allocator>();
    auto insIter = mInsertLog.begin();
    auto endIns = mInsertLog.end();
    const auto* pages = mPages.load();
    auto numPages = pages->size();
    std::vector<Iterator> result;
    result.reserve(numThreads);
    size_t beginIdx = 0;
    auto mod = numPages % numThreads;
    for (decltype(numThreads) i = 0; i < numThreads; ++i) {
        const auto& startIter = (i == numThreads - 1 ? insIter : endIns);
        auto endIdx = beginIdx + numPages / numThreads + (i < mod ? 1 : 0);
        result.emplace_back(alloc, pages, beginIdx, endIdx, startIter, endIns, &mPageManager, &mRecord);
        beginIdx = endIdx;
    }
    return result;
}

void Table::runGC(uint64_t minVersion) {
    allocator _;
    auto hashTable = mHashTable.load()->modifier(_);
    // we need to process the insert-log first. There might be delted
    // records which have an insert
    auto insBegin = mInsertLog.begin();
    auto insIter = insBegin;
    auto end = mInsertLog.end();
    auto updateBegin = mUpdateLog.end();
    auto updateEnd = mUpdateLog.end();
    InsertMap insertMap;
    for (; insIter != end && insIter->sealed(); ++insIter) {
        CDMRecord rec(insIter->data());
        if (!rec.isValidDataRecord()) continue;
        auto k = rec.key();
        insertMap[InsertMapKey(k)].push_back(insIter->data());
    }
    auto& roPages = *mPages.load();
    auto nPagesPtr = new (malloc(sizeof(PageList))) PageList(roPages);
    auto& nPages = *nPagesPtr;
    auto fillPage = reinterpret_cast<char*>(mPageManager.alloc());
    PageList newPages;
    // this loop just iterates over all pages
    for (size_t i = 0; i < nPages.size(); ++i) {
        Page page(mPageManager, nPages[i]);
        bool done;
        nPages[i] = page.gc(minVersion, insertMap, fillPage, done, hashTable);
        while (!done) {
            if (nPages[i]) {
                newPages.push_back(nPages[i]);
            }
            fillPage = reinterpret_cast<char*>(mPageManager.alloc());
            nPages[i] = page.gc(minVersion, insertMap, fillPage, done, hashTable);
        }
        if (nPages[i] == nullptr) {
            // This means that this page got merged with the older page.
            // Therefore we can remove it from the list
            nPages.erase(nPages.begin() + i);
        }
    }
    // now we can process the inserts
    while (!insertMap.empty()) {
        fillPage = reinterpret_cast<char*>(mPageManager.alloc());
        Page::fillWithInserts(minVersion, insertMap, fillPage, hashTable);
        nPages.push_back(fillPage);
    }
    // The garbage collection is finished - we can now reset the read only table
    mPages.store(nPagesPtr);
    {
        auto ht = mHashTable.load();
        mHashTable.store(hashTable.done());
        allocator::free(ht, [ht](){ ht->~CuckooTable(); });
    }
    while (!mInsertLog.truncateLog(insIter.page(), end.page()));
    while (!mUpdateLog.truncateLog(updateBegin.page(), updateEnd.page())); 
}

void GarbageCollector::run(const std::vector<Table*>& tables, uint64_t minVersion) {
    for (auto table : tables) {
        table->runGC(minVersion);
    }
}

} // namespace deltamain


StoreImpl<Implementation::DELTA_MAIN_REWRITE>::StoreImpl(const StorageConfig& config)
    : mPageManager(new (allocator::malloc(sizeof(PageManager))) PageManager(config.totalMemory), [](PageManager* p){ allocator::free_in_order(p, [p](){p->~PageManager();}); })
    , tableManager(*mPageManager, config, gc, commitManager)
{
}

StoreImpl<Implementation::DELTA_MAIN_REWRITE>::StoreImpl(const StorageConfig& config, size_t totalMem)
    : mPageManager(new (allocator::malloc(sizeof(PageManager))) PageManager(config.totalMemory), [](PageManager* p){ allocator::free_in_order(p, [p](){p->~PageManager();}); })
    , tableManager(*mPageManager, config, gc, commitManager)
{
}

} // namespace store
} // namespace tell

