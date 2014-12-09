#pragma once

#include <crossbow/string.hpp>
#include <config.h>
#include <implementation.hpp>
#include <util/Record.hpp>
#include <util/PageManager.hpp>
#include <util/TableManager.hpp>
#include <util/Log.hpp>
#include <util/CuckooHash.hpp>
#include <util/LogOperations.hpp>

namespace tell {
namespace store {
namespace dmrewrite {

/**
* A record in this approach has the following form:
* - 8 bytes: pointer to an entry on the log (atomic)
* - A multi-version record
*/
struct DMRecord {
    MultiVersionRecord multiVersionRecord;
    Record record;

    DMRecord(const Schema& schema);

    const char* getRecordData(const SnapshotDescriptor& snapshot,
                              const char* data,
                              bool& isNewest,
                              std::atomic<LogEntry*>** next = nullptr) const;
    bool needGCWork(const char* data, uint64_t minVersion) const;
    LogEntry* getNewest(const char* data) const;
    bool setNewest(LogEntry* old, LogEntry* n, const char* data);

    /**
    * This function will compact and merge the given tuple. It will return the following:
    * - The size of the old tuple, or 0 if the tuple can be deleted
    * - A pinter to an allocated chunk, if the tuple did not have enough room in the
    *   provided space
    */
    template<class Allocator>
    std::pair<size_t, char*> compactAndMerge(char* data, uint64_t minVersion, Allocator& allocator) const;
};

class Table {
    PageManager& mPageManager;
    Schema mSchema;
    DMRecord mRecord;
    Log mLog;
    Log mInsertLog;
    std::atomic<CuckooTable*> mHashMap;
    std::atomic<std::vector<char*>*> mPages;
public:
    Table(PageManager& pageManager, const Schema& schema);

    void insert(uint64_t key,
                const char* const data,
                const SnapshotDescriptor& descr,
                bool* succeeded = nullptr);

    bool get(uint64_t key, const char*& data, const SnapshotDescriptor& desc, bool& isNewest);

    bool update(uint64_t key, const char* data, const SnapshotDescriptor& snapshot);

    bool remove(uint64_t key, const SnapshotDescriptor& snapshot);
    void runGC(uint64_t minVersion);
private:
    bool generalUpdate(uint64_t key, LoggedOperation& loggedOperation, const SnapshotDescriptor& snapshot);
};

class GarbageCollector {
public:
    void run(const std::vector<Table*>& tables);
};

}

template<>
struct StoreImpl<Implementation::DELTA_MAIN_REWRITE> {
    using Table = dmrewrite::Table;
    using GC = dmrewrite::GarbageCollector;
    PageManager pageManager;
    GC gc;
    TableManager<Table, GC> tableManager;

    StoreImpl(const StorageConfig& config);

    StoreImpl(const StorageConfig& config, size_t totalMem);
};

} // namespace store
} // namespace tell
