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
#include "DMRecord.hpp"

namespace tell {
namespace store {
namespace dmrewrite {

class Table {
    struct PageHolder {
        char* page;
        PageHolder(char* page) : page(page) {}
    };
    PageManager& mPageManager;
    Schema mSchema;
    DMRecord mRecord;
    Log mLog;
    Log mInsertLog;
    std::atomic<CuckooTable*> mHashMap;
    std::atomic<std::vector<PageHolder*>*> mPages;
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
