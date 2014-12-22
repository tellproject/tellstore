#include <util/Record.hpp>
#include <util/LogOperations.hpp>
#include <util/chunk_allocator.hpp>
#include <unordered_set>
#include <map>
#include <tclDecls.h>
#include <Foundation/Foundation.h>
#include "dmrewrite.hpp"
#include "Page.hpp"

namespace tell {
namespace store {
namespace dmrewrite {

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
            const LogEntry* next = mRecord.getNewest(newest);
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
    loggedOperation.version = snapshot.version();
    loggedOperation.tuple = data;
    return generalUpdate(key, loggedOperation, snapshot);
}

bool Table::remove(uint64_t key, const SnapshotDescriptor& snapshot) {
    LoggedOperation loggedOperation;
    loggedOperation.operation = LogOperation::DELETE;
    loggedOperation.key = key;
    loggedOperation.version = snapshot.version();
    return generalUpdate(key, loggedOperation, snapshot);
}

bool Table::generalUpdate(uint64_t key, LoggedOperation& loggedOperation, SnapshotDescriptor const& snapshot) {
    auto& hMap = *mHashMap.load();
    auto addr = reinterpret_cast<char*>(hMap.get(key));
    if (addr) {
        // found it in the hash table
        LogEntry* newestPtr;
        bool isNewest;
        auto rec = mRecord.getRecordData(snapshot, addr, isNewest, &newestPtr);
        if (!isNewest) {
            // We got a conflict
            return false;
        }
        if (newestPtr != nullptr && newestPtr != LoggedOperation::loggedOperationFromTuple(rec)) {
            // Someone did an update in the mean time
            return false;
        }
        loggedOperation.previous = newestPtr;
        auto logEntry = mLog.append(uint32_t(loggedOperation.serializedSize()));
        loggedOperation.serialize(logEntry->data());
        logEntry->seal();
        return mRecord.setNewest(newestPtr, logEntry, addr);
    }
    auto iterator = mInsertLog.tail();
    while (iterator->sealed() && iterator->size > 0) {
        if (LoggedOperation::getKey(iterator->data()) == key) {
            const char* opData = iterator->data();
            //isNewest = true;
            if (!snapshot.inReadSet(LoggedOperation::getVersion(opData))) {
                return false;
            }
            auto newestPage = LoggedOperation::getNewest(opData);
            auto newest = mRecord.getNewest(newestPage);
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
            return mRecord.setNewest(newest, logEntry, newestPage);
        }
        iterator = iterator->next();
    }
    return false;
}

void Table::runGC(uint64_t minVersion) {
    crossbow::chunk_allocator<> allocator;
    std::vector<PageHolder*>* currPages = mPages.load();
    std::vector<PageHolder*>* newPages;
    if (currPages) {
        newPages = new(allocator::malloc(sizeof(std::vector<PageHolder*>))) std::vector<PageHolder*>(*currPages);
    } else {
        newPages = new(allocator::malloc(sizeof(std::vector<PageHolder*>))) std::vector<PageHolder*>();
    }
    using map_type = std::map<size_t, char*>;
    using allocator_type = crossbow::copy_allocator<map_type::value_type>;
    allocator_type alloc(allocator);
    std::map<size_t, char*, std::less<size_t>, allocator_type> freeMap(alloc);
    std::vector<PageHolder*, typename allocator_type::rebind<PageHolder*>> pagesToDelete(alloc);
    // tuple of pointers to the newest tuple, format is from -> to
    using charVecAllocator = allocator_type::rebind<std::pair<char*, char*>>;
    std::vector<std::pair<char*, char*>, charVecAllocator> newestPointersToChange(alloc);
    auto& pageList = *newPages;
    for (size_t i = 0; i < pageList.size(); ++i) {
        Page page(pageList[i]->page);
        Page nPage(pageList[i]->page);
        // we need to iterate over both pages to make sure we change the newest pointer in the old records
        auto old_iterator = page.begin();
        for (auto iterator = page.begin(); iterator != page.end(); ++iterator) {
            auto record = page.getRecord(iterator);
            if (!mRecord.needGCWork(record, minVersion)) {
                if ((*currPages)[i] != (*newPages)[i]) {
                    newestPointersToChange.emplace_back(page.getRecord(old_iterator), nPage.getRecord(iterator));
                }
                continue;
            }
            if ((*currPages)[i] == (*newPages)[i]) {
                // To clean a page, we need to copy it
                auto newPage = reinterpret_cast<char*>(mPageManager.alloc());
                memcpy(newPage, pageList[i]->page, TELL_PAGE_SIZE);
                pagesToDelete.push_back(pageList[i]);
                pageList[i] = new (tell::store::allocator::malloc(sizeof(PageHolder))) PageHolder(newPage);
                nPage = Page(newPage);
                // fill the pointers to change map up to the current position
                for (auto k = page.begin(), j = nPage.begin(); k != iterator; ++k) {
                    newestPointersToChange.emplace_back(page.getRecord(k), nPage.getRecord(j));
                    ++j;
                }
                // at this moment, we need to set the new iterator
                page = Page(pageList[i]->page);
                iterator = page.fromPosition(iterator);
                record = page.getRecord(iterator);
            }
            // we need to clean this page. This means, that there is either a newer version availabel,
            // there are versions which can be deleted, or both.
            // We do always shrink first. This has the advantage, that we need to move less often
            // records to new pages.
            auto p = mRecord.compactAndMerge(record, minVersion, alloc);
            if (p.second != nullptr) {
                freeMap.insert(p);
            }
            ++old_iterator;
        }
        // TODO: fill page up
        // Change the newest pointers
        for (auto& p : newestPointersToChange) {
            auto from = reinterpret_cast<std::atomic<char*>*>(p.first);
            auto to = reinterpret_cast<std::atomic<char*>*>(p.second);
            while (true) {
                auto fromPtr = from->load();
                auto toPtr = to->load();
                auto fromPtrVal = reinterpret_cast<unsigned long>(fromPtr);
                if (fromPtrVal % 4 == 0 && fromPtr != toPtr) {
                    to->store(fromPtr);
                    continue;
                }
                if (from->compare_exchange_strong(fromPtr, reinterpret_cast<char*>(to) + 1)) break;
            }
        }
        newestPointersToChange.clear();
    }
    for (auto p : pagesToDelete) {
        tell::store::allocator::free(p, [this, p]{
            mPageManager.free(p->page);
        });
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
