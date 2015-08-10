#pragma once

#include "Table.hpp"

#include <config.h>
#include <util/PageManager.hpp>
#include <util/StoreImpl.hpp>
#include <util/TableManager.hpp>
#include <util/VersionManager.hpp>

#include <crossbow/non_copyable.hpp>
#include <crossbow/string.hpp>

namespace tell {
namespace commitmanager {
class SnapshotDescriptor;
} // namespace commitmanager

namespace store {

class ScanQuery;

template<>
struct StoreImpl<Implementation::DELTA_MAIN_REWRITE> : crossbow::non_copyable, crossbow::non_movable {
    using Table = deltamain::Table;
    using GC = deltamain::GarbageCollector;

    StoreImpl(const StorageConfig& config)
        : mPageManager(PageManager::construct(config.totalMemory))
        , tableManager(*mPageManager, config, gc, mVersionManager)
    {
    }


    StoreImpl(const StorageConfig& config, size_t totalMem)
        : mPageManager(PageManager::construct(totalMem))
        , tableManager(*mPageManager, config, gc, mVersionManager)
    {
    }

    bool createTable(const crossbow::string &name,
                     const Schema& schema,
                     uint64_t& idx)
    {
        return tableManager.createTable(name, schema, idx);
    }

    const Table* getTable(uint64_t id) const
    {
        return tableManager.getTable(id);
    }

    const Table* getTable(const crossbow::string& name, uint64_t& id) const
    {
        return tableManager.getTable(name, id);
    }

    bool get(uint64_t tableId,
             uint64_t key,
             size_t& size,
             const char*& data,
             const commitmanager::SnapshotDescriptor& snapshot,
             uint64_t& version,
             bool& isNewest)
    {
        return tableManager.get(tableId, key, size, data, snapshot, version, isNewest);
    }

    bool update(uint64_t tableId,
                uint64_t key,
                size_t size,
                const char* const data,
                const commitmanager::SnapshotDescriptor& snapshot)
    {
        return tableManager.update(tableId, key, size, data, snapshot);
    }

    void insert(uint64_t tableId,
                uint64_t key,
                size_t size,
                const char* const data,
                const commitmanager::SnapshotDescriptor& snapshot,
                bool* succeeded = nullptr)
    {
        tableManager.insert(tableId, key, size, data, snapshot, succeeded);
    }

    bool remove(uint64_t tableId,
                uint64_t key,
                const commitmanager::SnapshotDescriptor& snapshot)
    {
        return tableManager.remove(tableId, key, snapshot);
    }

    bool revert(uint64_t tableId,
                uint64_t key,
                const commitmanager::SnapshotDescriptor& snapshot)
    {
        return tableManager.revert(tableId, key, snapshot);
    }

    bool scan(uint64_t tableId, ScanQuery* query)
    {
        return tableManager.scan(tableId, query);
    }

    /**
     * We use this method mostly for test purposes. But
     * it might be handy in the future as well. If possible,
     * this should be implemented in an efficient way.
     */
    void forceGC()
    {
        tableManager.forceGC();
    }

private:
    PageManager::Ptr mPageManager;
    GC gc;
    VersionManager mVersionManager;
    TableManager<Table, GC> tableManager;

};

} // namespace store
} // namespace tell
