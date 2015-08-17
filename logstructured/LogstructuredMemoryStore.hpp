#pragma once

#include "Table.hpp"

#include <config.h>
#include <util/PageManager.hpp>
#include <util/StoreImpl.hpp>
#include <util/TableManager.hpp>
#include <util/VersionManager.hpp>

#include <crossbow/non_copyable.hpp>
#include <crossbow/string.hpp>

#include <cstdint>

namespace tell {
namespace commitmanager {
class SnapshotDescriptor;
} // namespace commitmanager

namespace store {

class ScanQuery;

/**
 * @brief A Storage implementation using a Log-Structured Memory approach as its data store
 */
template<>
struct StoreImpl<Implementation::LOGSTRUCTURED_MEMORY> : crossbow::non_copyable, crossbow::non_movable {
public:
    using Table = logstructured::Table;
    using GC = Table::GarbageCollector;

    StoreImpl(const StorageConfig& config)
            : mPageManager(PageManager::construct(config.totalMemory)),
              mGc(*this),
              mTableManager(*mPageManager, config, mGc, mVersionManager),
              mHashMap(config.hashMapCapacity) {
    }

    bool createTable(const crossbow::string& name, const Schema& schema, uint64_t& idx) {
        return mTableManager.createTable(name, schema, idx, mVersionManager, mHashMap);
    }

    const Table* getTable(uint64_t id) const {
        return mTableManager.getTable(id);
    }

    const Table* getTable(const crossbow::string& name, uint64_t& id) const {
        return mTableManager.getTable(name, id);
    }

    bool get(uint64_t tableId, uint64_t key, size_t& size, const char*& data,
            const commitmanager::SnapshotDescriptor& snapshot, uint64_t& version, bool& isNewest) {
        return mTableManager.get(tableId, key, size, data, snapshot, version, isNewest);
    }

    bool update(uint64_t tableId, uint64_t key, size_t size, const char* data,
            const commitmanager::SnapshotDescriptor& snapshot) {
        return mTableManager.update(tableId, key, size, data, snapshot);
    }

    void insert(uint64_t tableId, uint64_t key, size_t size, const char* data,
            const commitmanager::SnapshotDescriptor& snapshot, bool* succeeded = nullptr) {
        mTableManager.insert(tableId, key, size, data, snapshot, succeeded);
    }

    bool remove(uint64_t tableId, uint64_t key, const commitmanager::SnapshotDescriptor& snapshot) {
        return mTableManager.remove(tableId, key, snapshot);
    }

    bool revert(uint64_t tableId, uint64_t key, const commitmanager::SnapshotDescriptor& snapshot) {
        return mTableManager.revert(tableId, key, snapshot);
    }

    bool scan(uint64_t tableId, ScanQuery* query) {
        return mTableManager.scan(tableId, query);
    }

    /**
     * We use this method mostly for test purposes. But
     * it might be handy in the future as well. If possible,
     * this should be implemented in an efficient way.
     */
    void forceGC() {
        mTableManager.forceGC();
    }

private:
    PageManager::Ptr mPageManager;
    GC mGc;
    VersionManager mVersionManager;
    TableManager<Table, GC> mTableManager;

    Table::HashTable mHashMap;
};

} // namespace store
} // namespace tell
