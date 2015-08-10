#include "Table.hpp"
#include "Record.hpp"
#include "Page.hpp"
#include "InsertMap.hpp"

#include <commitmanager/SnapshotDescriptor.hpp>

#include <memory>

namespace tell {
namespace store {
namespace deltamain {


Table::ScanProcessor::ScanProcessor(const std::shared_ptr<crossbow::allocator>& alloc,
        const PageList* pages,
        size_t pageIdx,
        size_t pageEndIdx,
        const LogIterator& logIter,
        const LogIterator& logEnd,
        PageManager* pageManager,
        const char* queryBuffer,
        const std::vector<ScanQuery*>& queryData,
        const Record* record)
    : mAllocator(alloc)
    , pages(pages)
    , pageIdx(pageIdx)
    , pageEndIdx(pageEndIdx)
    , logIter(logIter)
    , logEnd(logEnd)
    , pageManager(pageManager)
    , query(queryBuffer, queryData)
    , record(record)
    , pageIter(Page(*pageManager, (*pages)[pageIdx]).begin())
    , pageEnd (Page(*pageManager, (*pages)[pageIdx]).end())
    , currKey(0u)
{
}

void Table::ScanProcessor::process()
{
    for (setCurrentEntry(); currVersionIter.isValid(); next()) {
        query.processRecord(*currVersionIter->record(), currKey, currVersionIter->data(), currVersionIter->size(),
                currVersionIter->validFrom(), currVersionIter->validTo());
    }
}

void Table::ScanProcessor::next()
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

void Table::ScanProcessor::setCurrentEntry()
{
    while (logIter != logEnd) {
        if (logIter->sealed()) {
            CDMRecord rec(logIter->data());
            if (rec.isValidDataRecord()) {
                currKey = rec.key();
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
                currKey = rec.key();
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
    , mRecord(schema)
    , mHashTable(crossbow::allocator::construct<CuckooTable>(pageManager))
    , mInsertLog(pageManager)
    , mUpdateLog(pageManager)
    , mPages(crossbow::allocator::construct<PageList>())
#if defined USE_COLUMN_MAP
    ,
    mNumberOfFixedSizedFields(schema.fixedSizeFields().size()),
    mNumberOfVarSizedFields(schema.varSizeFields().size())
#endif
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
            data = rec.data(snapshot, size, version, isNewest, isValid, &wasDeleted);
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
#if defined USE_ROW_STORE
        rec.data(snapshot, s, version, isNewest, isValid, &wasDeleted);
#elif defined USE_COLUMN_MAP
        rec.data(snapshot, s, version, isNewest, isValid, &wasDeleted, this, false);
#else
#error "Unknown storage layout"
#endif

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
#if defined USE_ROW_STORE
    return rec.update(nextPtr, isValid, snapshot);
#elif defined USE_COLUMN_MAP
    return rec.update(nextPtr, isValid, snapshot, this);
#else
#error "Unknown storage layout"
#endif
}

std::vector<Table::ScanProcessor> Table::startScan(int numThreads, const char* queryBuffer,
        const std::vector<ScanQuery*>& queries) const
{
    auto alloc = std::make_shared<crossbow::allocator>();
    auto insIter = mInsertLog.begin();
    auto endIns = mInsertLog.end();
    const auto* pages = mPages.load();
    auto numPages = pages->size();
    std::vector<ScanProcessor> result;
    result.reserve(numThreads);
    size_t beginIdx = 0;
    auto mod = numPages % numThreads;
    for (decltype(numThreads) i = 0; i < numThreads; ++i) {
        const auto& startIter = (i == numThreads - 1 ? insIter : endIns);
        auto endIdx = beginIdx + numPages / numThreads + (i < mod ? 1 : 0);
        result.emplace_back(alloc, pages, beginIdx, endIdx, startIter, endIns, &mPageManager, queryBuffer, queries,
                &mRecord);
        beginIdx = endIdx;
    }
    return result;
}

void Table::runGC(uint64_t minVersion) {
    crossbow::allocator _;
    auto hashTable = mHashTable.load()->modifier();
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
    auto nPagesPtr = crossbow::allocator::construct<PageList>(roPages);
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
    {
        auto p = mPages.load();
        mPages.store(nPagesPtr);
        crossbow::allocator::destroy(p);
    }
    {
        auto ht = mHashTable.load();
        mHashTable.store(hashTable.done());
        crossbow::allocator::destroy(ht);
    }
    while (!mInsertLog.truncateLog(insIter.page(), end.page()));
    while (!mUpdateLog.truncateLog(updateBegin.page(), updateEnd.page())); 
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

