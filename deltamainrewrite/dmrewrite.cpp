#include <util/Record.hpp>
#include <util/LogOperations.hpp>
#include <util/chunk_allocator.hpp>
#include <unordered_set>
#include <map>
#include "dmrewrite.hpp"
#include "Page.hpp"

namespace tell {
namespace store {
namespace dmrewrite {


DMRecord::DMRecord(const Schema& schema)
    : record(schema) {
}

const char* DMRecord::getRecordData(const SnapshotDescriptor& snapshot,
                                    const char* data,
                                    bool& isNewest,
                                    std::atomic<LogEntry*>** next /*= nullptr*/) const {
    const char* res = multiVersionRecord.getRecord(snapshot, data + 8, isNewest);
    if (next) {
        *next = reinterpret_cast<std::atomic<LogEntry*>*>(const_cast<char*>(data));
    }
    if (isNewest) {
        // if the newest record is valid in the given snapshot, we need to check whether
        // there are newer versions on the log. Otherwise we are fine (even if there are
        // newer versions, we won't care).
        const LogEntry* loggedOperation = getNewest(data);
        while (loggedOperation) {
            // there are newer versions
            auto version = LoggedOperation::getVersion(loggedOperation->data());
            if (snapshot.inReadSet(version)) {
                return LoggedOperation::getRecord(loggedOperation->data());
            }
            // if we reach this code, we did not return the newest version
            isNewest = false;
            loggedOperation = LoggedOperation::getPrevious(loggedOperation->data());
        }
    }
    return res;
}

LogEntry* DMRecord::getNewest(const char* data) const {
    LogEntry* res = reinterpret_cast<std::atomic<LogEntry*>*>(const_cast<char*>(data))->load();
    unsigned long ptr = reinterpret_cast<unsigned long>(res);
    if (ptr % 2 != 0) {
        // the GC is running and set this already to the new pointer
        if (ptr % 4 != 0)
            ptr -= 4;
        char* newEntry = *reinterpret_cast<char**>(ptr + 1);
        res = reinterpret_cast<std::atomic<LogEntry*>*>(const_cast<char*>(newEntry))->load();
    } else if (ptr % 4 != 0) {
        return nullptr;
    }
    return res;
}

bool DMRecord::setNewest(LogEntry* old, LogEntry* n, const char* data) {
    std::atomic<LogEntry*>* en =  reinterpret_cast<std::atomic<LogEntry*>*>(const_cast<char*>(data));
    LogEntry* logEntry = en->load();
    unsigned long ptr = reinterpret_cast<unsigned long>(logEntry);
    if (ptr % 2 != 0) {
        char* newEntry = *reinterpret_cast<char**>(ptr + 1);
        en = reinterpret_cast<std::atomic<LogEntry*>*>(const_cast<char*>(newEntry));
        logEntry = en->load();
        ptr = reinterpret_cast<unsigned long>(logEntry);
    }
    if (ptr % 4 != 0) {
        if (old != nullptr) return false;
        return en->compare_exchange_strong(logEntry, n);
    }
    return en->compare_exchange_strong(old, n);
}

bool DMRecord::needGCWork(const char* data, uint64_t minVersion) const {
    return getNewest(data) != nullptr ||
        (MultiVersionRecord::getSmallestVersion(data + 8) < minVersion &&
            MultiVersionRecord::getNumberOfVersions(data) > 1);
}

template<class Allocator>
std::pair<size_t, char*> DMRecord::compactAndMerge(char* data, uint64_t minVersion, Allocator& allocator) const {
    using allocator_t = typename Allocator::template rebind<char>;
    allocator_t alloc(allocator);
    uint32_t oldSize = multiVersionRecord.compact(data + 8, minVersion);
    uint32_t tupleSize = multiVersionRecord.getSize(data + 8);
    if (oldSize == 0u) {
        return std::make_pair(0, nullptr);
    }
    std::pair<size_t, char*> res = std::make_pair(oldSize, nullptr);
    std::atomic<LogEntry*>& logEntryPtr = *reinterpret_cast<std::atomic<LogEntry*>*>(data);
    auto logEntry = logEntryPtr.load();
    if (logEntry == nullptr) {
        return res;
    }
    // there are updates on this element. In that case, it could be, that we can safely
    // delete the tuple and create a new one from the updates
    if (LoggedOperation::getVersion(logEntry->data()) > minVersion) {
        // We can rewrite the whole tuple
        std::vector<std::pair<uint64_t, const char*>, typename Allocator::template rebind<std::pair<uint64_t, const char*>>> versions(allocator);
        const LogEntry* current = logEntry;
        uint32_t recordsSize = 0u;
        while (current) {
            auto recVersion = LoggedOperation::getVersion(current->data());
            auto recData = LoggedOperation::getRecord(current->data());
            versions.push_back(std::make_pair(recVersion, recData));
            recordsSize += record.getSize(recData);
            current = LoggedOperation::getPrevious(current->data());
        }
        res.first = recordsSize + 8 + 8 + 8*versions.size() + 4*versions.size() + (versions.size() % 2 == 0 ? 0 : 4);
        char* ptr = data + 8;
        assert(res.second == nullptr);
        if (res.first > oldSize) {
            // we need to allocate a block, since we ran out of space here
            ptr = alloc.allocate(res.first);
            res.second = ptr;
        } else {
            // set the first 8 bytes (pointer to newest version, to 0
            memset(data, 0, 8);
        }
        return res;
    }
    return res;
}

Table::Table(PageManager& pageManager, Schema const& schema)
    : mPageManager(pageManager),
      mSchema(schema),
      mRecord(schema),
      mLog(mPageManager),
      mInsertLog(mPageManager),
      mHashMap(new(allocator::malloc(sizeof(CuckooTable))) CuckooTable(mPageManager)),
      mPages(nullptr)
{
}

void GarbageCollector::run(const std::vector<Table*>& tables) {
}

void Table::insert(uint64_t key, const char* const data, const SnapshotDescriptor& descr,
                   bool* succeeded /*=nullptr*/) {
    if (mHashMap.load()->get(key) != nullptr) {
        if (succeeded != nullptr)
            *succeeded = false;
        return;
    }
    // insertion works like follows:
    // - First we append the insertion to the log
    // - Then we check, whether this tuple has another entry in the log
    // - The insertion will still be in the log, but it will be ignored by the merger
    // By doing so, we can guarantee, that the key will be unique
    LoggedOperation op;
    op.key = key;
    op.operation = LogOperation::INSERT;
    op.tuple = data;
    auto nEntry = mInsertLog.append(uint32_t(op.serializedSize()));
    op.serialize(nEntry->data());
    nEntry->seal();
    if (succeeded != nullptr) {
        auto tail = mInsertLog.tail();
        while (tail != nEntry) {
            while (!tail->sealed()) {
            }
            if (key == *reinterpret_cast<const uint64_t*>(tail->data() + 4)) {
                *succeeded = false;
                return;
            }
            tail = tail->next();
        }
        *succeeded = true;
    }
    return;
}

bool Table::get(uint64_t key, const char*& data, const SnapshotDescriptor& desc, bool& isNewest) {
    auto& hMap = *mHashMap.load();
    auto addr = hMap.get(key);
    if (addr) {
        // found the value
        data = mRecord.getRecordData(desc, reinterpret_cast<const char*>(addr), isNewest);
        return true;
    }
    // The record is not in the in the hash map - therefore we need to look for it
    // in the insert log
    auto iterator = mInsertLog.tail();
    while (iterator->sealed() && iterator->size > 0) {
        if (LoggedOperation::getKey(iterator->data()) == key) {
            auto opData = iterator->data();
            isNewest = true;
            if (!desc.inReadSet(LoggedOperation::getVersion(opData))) {
                return false;
            }
            data = LoggedOperation::getRecord(opData);
            auto newest = LoggedOperation::getNewest(opData);
            const LogEntry* next = newest;
            while (next) {
                if (desc.inReadSet(LoggedOperation::getVersion(next->data()))) {
                    return true;
                }
                isNewest = false;
                next = LoggedOperation::getPrevious(next->data());
            }
            return true;
        }
        iterator = iterator->next();
    }
    return false;
}

bool Table::update(uint64_t key, const char* data, const SnapshotDescriptor& snapshot) {
    LoggedOperation loggedOperation;
    loggedOperation.operation = LogOperation::UPDATE;
    loggedOperation.key = key;
    loggedOperation.version = snapshot.version;
    loggedOperation.tuple = data;
    return generalUpdate(key, loggedOperation, snapshot);
}

bool Table::remove(uint64_t key, const SnapshotDescriptor& snapshot) {
    LoggedOperation loggedOperation;
    loggedOperation.operation = LogOperation::DELETE;
    loggedOperation.key = key;
    loggedOperation.version = snapshot.version;
    return generalUpdate(key, loggedOperation, snapshot);
}

bool Table::generalUpdate(uint64_t key, LoggedOperation& loggedOperation, SnapshotDescriptor const& snapshot) {
    auto& hMap = *mHashMap.load();
    auto addr = reinterpret_cast<char*>(hMap.get(key));
    if (addr) {
        // found it in the hash table
        std::atomic<LogEntry*>* newestPtr;
        bool isNewest;
        auto rec = mRecord.getRecordData(snapshot, addr, isNewest, &newestPtr);
        if (!isNewest) {
            // We got a conflict
            return false;
        }
        auto newestOp = newestPtr->load();
        if (newestOp != nullptr && newestOp != LoggedOperation::loggedOperationFromTuple(rec)) {
            // Someone did an update in the mean time
            return false;
        }
        loggedOperation.previous = newestPtr->load();
        auto logEntry = mLog.append(uint32_t(loggedOperation.serializedSize()));
        loggedOperation.serialize(logEntry->data());
        logEntry->seal();
        return newestPtr->compare_exchange_strong(newestOp, logEntry);
    }
    auto iterator = mInsertLog.tail();
    while (iterator->sealed() && iterator->size > 0) {
        if (LoggedOperation::getKey(iterator->data()) == key) {
            const char* opData = iterator->data();
            //isNewest = true;
            if (!snapshot.inReadSet(LoggedOperation::getVersion(opData))) {
                return false;
            }
            auto newest = LoggedOperation::getNewest(opData);
            auto newestPtr = newest;
            if (newestPtr) {
                if (!snapshot.inReadSet(LoggedOperation::getVersion(newestPtr->data()))) {
                    // The newest version is not in the read set
                    return false;
                }
            }
            // we found the newest record and it is writable
            loggedOperation.previous = newestPtr;
            auto logEntry = mLog.append(uint32_t(loggedOperation.serializedSize()));
            loggedOperation.serialize(logEntry->data());
            logEntry->seal();
            // TODO: IMPLEMENT!!
            //return newest->compare_exchange_strong(newestPtr, logEntry);
            return false;
        }
        iterator = iterator->next();
    }
    return false;
}

void Table::runGC(uint64_t minVersion) {
    crossbow::chunk_allocator<> allocator;
    std::vector<char*>* currPages = mPages.load();
    std::vector<char*>* newPages;
    if (currPages) {
        newPages = new(allocator::malloc(sizeof(std::vector<char *>))) std::vector<char *>(*currPages);
    } else {
        newPages = new(allocator::malloc(sizeof(std::vector<char *>))) std::vector<char *>();
    }
    using map_type = std::map<size_t, char*>;
    using allocator_type = crossbow::copy_allocator<map_type::value_type>;
    allocator_type alloc(allocator);
    std::map<size_t, char*, std::less<size_t>, allocator_type> freeMap(alloc);
    auto& pageList = *newPages;
    for (size_t i = 0; i < pageList.size(); ++i) {
        Page page(mPageManager, pageList[i]);
        for (auto iterator = page.begin(); iterator != page.end(); ++iterator) {
            // TODO: Clean pages
            auto record = page.getRecord(iterator);
            if (!mRecord.needGCWork(record, minVersion)) continue;
            // we need to clean this page. This means, that there is either a newer version availabel,
            // there are versions which can be deleted, or both.
            // We do always shrink first. This has the advantage, that we need to move less often
            // records to new pages.
            auto p = mRecord.compactAndMerge(record, minVersion, alloc);
            if (p.first != 0) {
                freeMap.insert(p);
            }
        }
    }
}
} // namespace tell
StoreImpl<Implementation::DELTA_MAIN_REWRITE>::StoreImpl(const StorageConfig& config)
    : pageManager(config.totalMemory), tableManager(config, gc) {
}

StoreImpl<Implementation::DELTA_MAIN_REWRITE>::StoreImpl(const StorageConfig& config, size_t totalMem)
    : pageManager(totalMem), tableManager(config, gc) {
}

} // namespace store
} // namespace dmrewrite
