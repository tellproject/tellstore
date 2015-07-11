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
#include <util/IteratorEntry.hpp>

#include <crossbow/allocator.hpp>
#include <crossbow/string.hpp>

#include <memory>
#include <vector>
#include <limits>
#include <atomic>
#include <functional>

#include "Page.hpp"

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
    class Iterator {
    public:
        using IteratorEntry = BaseIteratorEntry;
    private:
        friend class Table;
        using LogIterator = Log<OrderedLogImpl>::ConstLogIterator;
    private: // assigned members
        std::shared_ptr<crossbow::allocator> mAllocator;
        const PageList* pages;
        size_t pageIdx;
        size_t pageEndIdx;
        LogIterator logIter;
        LogIterator logEnd;
        PageManager* pageManager;
        const Record* record;
    private: // calculated members
        void setCurrentEntry();

        Page::Iterator pageIter;
        Page::Iterator pageEnd;
        CDMRecord::VersionIterator currVersionIter;
    public:
        Iterator(const std::shared_ptr<crossbow::allocator>& alloc,
                 const PageList* pages,
                 size_t pageIdx,
                 size_t pageEndIdx,
                 const LogIterator& logIter,
                 const LogIterator& logEnd,
                 PageManager* pageManager,
                 const Record* record);

        void next();

        bool done() {
            return !currVersionIter.isValid();
        }

        const IteratorEntry& value() {
            return *currVersionIter;
        }
    };
    Table(PageManager& pageManager, const Schema& schema, uint64_t idx);

    ~Table();

    const Schema& schema() const {
        return mSchema;
    }

    TableType type() const {
        return mSchema.type();
    }

    bool get(uint64_t key,
             size_t& size,
             const char*& data,
             const SnapshotDescriptor& snapshot,
             uint64_t& version,
             bool& isNewest) const;

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

    bool revert(uint64_t key,
                const SnapshotDescriptor& snapshot);

    void runGC(uint64_t minVersion);

    std::vector<Iterator> startScan(int numThreads) const;
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
    PageManager::Ptr mPageManager;
    GC gc;
    CommitManager commitManager;
    TableManager<Table, GC> tableManager;

    StoreImpl(const StorageConfig& config);

    StoreImpl(const StorageConfig& config, size_t totalMem);

    PageManager& pageManager() {
        return *(mPageManager.get());
    }

    Transaction startTx()
    {
        return Transaction(*this, commitManager.startTx());
    }

    bool createTable(const crossbow::string &name,
                     const Schema& schema,
                     uint64_t& idx)
    {
        return tableManager.createTable(name, schema, idx);
    }

    const Table* getTable(const crossbow::string& name, uint64_t& id) const {
        return tableManager.getTable(name, id);
    }

    bool get(uint64_t tableId,
             uint64_t key,
             size_t& size,
             const char*& data,
             const SnapshotDescriptor& snapshot,
             uint64_t& version,
             bool& isNewest)
    {
        return tableManager.get(tableId, key, size, data, snapshot, version, isNewest);
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

    bool remove(uint64_t tableId,
                uint64_t key,
                const SnapshotDescriptor& snapshot)
    {
        return tableManager.remove(tableId, key, snapshot);
    }

    bool revert(uint64_t tableId,
                uint64_t key,
                const SnapshotDescriptor& snapshot)
    {
        return tableManager.revert(tableId, key, snapshot);
    }

    int numScanThreads() const {
        return tableManager.numScanThreads();
    }

    bool scan(uint64_t tableId, char* query, size_t querySize, const std::vector<ScanQueryImpl*>& impls) {
        return tableManager.scan(tableId, query, querySize, impls);
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

    void commit(SnapshotDescriptor& snapshot)
    {
        commitManager.commitTx(snapshot);
    }

    void abort(SnapshotDescriptor& snapshot)
    {
        // TODO: Roll-back. I am not sure whether this would generally
        // work. Probably not (since we might also need to roll back the
        // index which has to be done in the processing layer).
        commitManager.abortTx(snapshot);
    }

};
} // namespace store
} // namespace tell
