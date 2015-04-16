#include "Table.hpp"
#include "Record.hpp"
#include "Page.hpp"


namespace tell {
namespace store {
namespace deltamain {

Table::Table(PageManager& pageManager, const Schema& schema)
    : mPageManager(pageManager)
    , mSchema(schema)
    , mRecord(schema)
    , mHashTable(pageManager)
    , mInsertLog(pageManager)
    , mUpdateLog(pageManager)
    , mPages(new (allocator::malloc(sizeof(std::vector<char*>))) std::vector<char*>())
{}

bool Table::get(uint64_t key,
                size_t& size,
                const char*& data,
                const SnapshotDescriptor& snapshot,
                bool& isNewest) const {
    auto ptr = mHashTable.get(key);
    if (ptr) {
        CDMRecord rec(reinterpret_cast<char*>(ptr));
        bool wasDeleted;
        data = rec.data(snapshot, size, isNewest, &wasDeleted);
        return !wasDeleted;
    }
    // in this case we need to scan through the insert log
    for (auto iter = mInsertLog.begin(); iter != mInsertLog.end(); ++iter) {
        if (!iter->sealed()) continue;
        CDMRecord rec(iter->data());
        if (rec.key() == key) {
            bool wasDeleted;
            data = rec.data(snapshot, size, isNewest, &wasDeleted);
            return !wasDeleted;
        }
    }
    // in this case the tuple does not exist
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
    auto iterEnd = mInsertLog.end();
    auto ptr = mHashTable.get(key);
    if (ptr) {
        // the key exists... but it could be, that it got deleted
        CDMRecord rec(reinterpret_cast<const char*>(ptr));
        bool wasDeleted, isNewest;
        size_t s;
        rec.data(snapshot, s, isNewest, &wasDeleted);
        if (!(wasDeleted && isNewest)) {
            if (succeeded) *succeeded = false;
            return;
        }
        // the tuple was deleted and we don't have a
        // write-write conflict, therefore we can continue
        // with the insert
        // But we only need to write the previous pointer if it
        // is on the update log.
        if (rec.typeOfNewestVersion() != CDMRecord::Type::LOG_UPDATE) ptr = nullptr;
    }
    // To do an insert, we optimistically append it to the log.
    // Then we check for conflicts iff the user wants to know whether
    // the insert succeeded.
    auto logEntrySize = size + DMRecord::spaceOverhead(DMRecord::Type::LOG_INSERT);
    auto entry = mInsertLog.append(logEntrySize);
    {
        // We do this in another scope, after this scope is closed, the log
        // is read only (when seal is called)
        DMRecord insertRecord(entry->data());
        insertRecord.setType(DMRecord::Type::LOG_INSERT);
        insertRecord.writeKey(key);
        insertRecord.writeVersion(snapshot.version());
        insertRecord.writePrevious(reinterpret_cast<const char*>(ptr));
        insertRecord.writeNextPtr(nullptr);
        insertRecord.writeData(size, data);
    }
    entry->seal();
    if (succeeded == nullptr) return;
    while (iter != iterEnd) {
        // we busy wait if the entry was not sealed
        while (!iter->sealed()) {}
        const LogEntry* en = iter.operator->();
        if (en == entry) {
            *succeeded = true;
            return;
        }
        CDMRecord rec(en->data());
        if (rec.key() == key) {
            *succeeded = false;
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

template<class Fun>
bool Table::genericUpdate(const Fun& appendFun,
                          uint64_t key,
                          const SnapshotDescriptor& snapshot)
{
    auto iter = mInsertLog.begin();
    auto iterEnd = mInsertLog.end();
    auto ptr = mHashTable.get(key);
    if (!ptr) {
        while (iter != iterEnd) {
            CDMRecord rec(iter->data());
            if (rec.key() == key) {
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
    return rec.update(nextPtr, snapshot);
}

void Table::runGC(uint64_t) {
    // TODO: Implement
    // mark the whole gc phase as an epoch
    allocator _;
    auto& roPages = *mPages.load();
    auto nPagesPtr = new (malloc(sizeof(PageList))) PageList(roPages);
    auto& nPages = *nPagesPtr;
    // this loop just iterates over all pages
    for (int i = 0; i < nPages.size(); ++i) {
        Page page(nPages[i]);
        nPages[i] = page.gc();
    }
    mPages.store(nPagesPtr);
}

void GarbageCollector::run(const std::vector<Table*>& tables, uint64_t minVersion) {
    for (auto table : tables) {
        table->runGC(minVersion);
    }
}

} // namespace deltamain


StoreImpl<Implementation::DELTA_MAIN_REWRITE>::StoreImpl(const StorageConfig& config)
    : pageManager(config.totalMemory), tableManager(pageManager, config, gc, commitManager) {
}

StoreImpl<Implementation::DELTA_MAIN_REWRITE>::StoreImpl(const StorageConfig& config, size_t totalMem)
    : pageManager(totalMem), tableManager(pageManager, config, gc, commitManager) {
}

} // namespace store
} // namespace tell

