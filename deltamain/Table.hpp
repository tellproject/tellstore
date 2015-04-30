#pragma once
#include <config.h>
#include <implementation.hpp>
#include <util/PageManager.hpp>
#include <util/TransactionImpl.hpp>
#include <util/CommitManager.hpp>
#include <util/TableManager.hpp>
#include <util/CuckooHash.hpp>
#include <util/Log.hpp>
#include <util/Record.hpp>

#include <vector>
#include <limits>
#include <atomic>
#include <crossbow/string.hpp>

namespace tell {
namespace store {
namespace deltamain {

class Table {
    using PageList = std::vector<char*>;
    PageManager& mPageManager;
    Schema mSchema;
    Record mRecord;
    std::atomic<CuckooTable*> mHashTable;
    Log<OrderedLogImpl> mInsertLog;
    Log<OrderedLogImpl> mUpdateLog;
    std::atomic<PageList*> mPages;
public:
    Table(PageManager& pageManager, const Schema& schema);
    bool get(uint64_t key,
             size_t& size,
             const char*& data,
             const SnapshotDescriptor& snapshot,
             bool& isNewest) const;

    void insert(uint64_t key,
                const GenericTuple& tuple,
                const SnapshotDescriptor& snapshot,
                bool* succeeded = nullptr);
    void insert(uint64_t key,
                size_t size,
                const char* const data,
                const SnapshotDescriptor& snapshot,
                bool* succeeded = nullptr);

    bool update(uint64_t key,
                size_t size,
                const char* const data,
                const SnapshotDescriptor& snapshot);

    bool remove(uint64_t key,
                const SnapshotDescriptor& snapshot);

    void runGC(uint64_t minVersion);
private:
    template<class Fun>
    bool genericUpdate(const Fun& appendFun,
                       uint64_t key,
                       const SnapshotDescriptor& snapshot);
};

class GarbageCollector {
public:
    void run(const std::vector<Table*>& tables, uint64_t minVersion);
};

} // namespace deltamain

template<>
struct StoreImpl<Implementation::DELTA_MAIN_REWRITE> {
    using Table = deltamain::Table;
    using GC = deltamain::GarbageCollector;
    using StorageType = StoreImpl<Implementation::DELTA_MAIN_REWRITE>;
    using Transaction = TransactionImpl<StorageType>;
    PageManager pageManager;
    GC gc;
    CommitManager commitManager;
    TableManager<Table, GC> tableManager;

    StoreImpl(const StorageConfig& config);

    StoreImpl(const StorageConfig& config, size_t totalMem);


    Transaction startTx()
    {
        return Transaction(*this, commitManager.startTx());
    }

    bool creteTable(const crossbow::string &name,
                    const Schema& schema,
                    uint64_t& idx)
    {
        return tableManager.createTable(name, schema, idx);
    }

    bool getTableId(const crossbow::string&name, uint64_t& id) {
        return tableManager.getTableId(name, id);
    }

    bool get(uint64_t tableId,
             uint64_t key,
             size_t& size,
             const char*& data,
             const SnapshotDescriptor& snapshot,
             bool& isNewest)
    {
        return tableManager.get(tableId, key, size, data, snapshot, isNewest);
    }

    bool update(uint64_t tableId,
                uint64_t key,
                size_t size,
                const char* const data,
                const SnapshotDescriptor& snapshot)
    {
        return tableManager.update(tableId, key, size, data, snapshot);
    }

    void insert(uint64_t tableId,
                uint64_t key,
                size_t size,
                const char* const data,
                const SnapshotDescriptor& snapshot,
                bool* succeeded = nullptr)
    {
        tableManager.insert(tableId, key, size, data, snapshot, succeeded);
    }

    void insert(uint64_t tableId,
                uint64_t key,
                const GenericTuple& tuple,
                const SnapshotDescriptor& snapshot,
                bool* succeeded = nullptr)
    {
        tableManager.insert(tableId, key, tuple, snapshot, succeeded);
    }

    bool remove(uint64_t tableId,
                uint64_t key,
                const SnapshotDescriptor& snapshot)
    {
        return tableManager.remove(tableId, key, snapshot);
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

    template<class T>
    void commit(T& transaction)
    {
        commitManager.commitTx(transaction.mDescriptor);
        transaction.mCommitted = true;
    }

    void abort(Transaction& transaction)
    {
        // TODO: Roll-back. I am not sure whether this would generally
        // work. Probably not (since we might also need to roll back the
        // index which has to be done in the processing layer).
        commitManager.abortTx(transaction.mDescriptor);
        transaction.mCommitted = true;
    }

};
} // namespace store
} // namespace tell
