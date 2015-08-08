#pragma once
#include <config.h>
#include <util/PageManager.hpp>
#include <util/TableManager.hpp>
#include <util/CuckooHash.hpp>
#include <util/Log.hpp>
#include <util/ScanQuery.hpp>
#include <util/StoreImpl.hpp>
#include <util/VersionManager.hpp>

#include <tellstore/Record.hpp>

#include <crossbow/allocator.hpp>
#include <crossbow/string.hpp>

#include <memory>
#include <vector>
#include <limits>
#include <atomic>
#include <functional>

#include "Page.hpp"

namespace tell {
namespace commitmanager {
class SnapshotDescriptor;
} // namespace commitmanager

namespace store {
namespace deltamain {

class Table {
    using PageList = std::vector<char*>;
    PageManager& mPageManager;
    Record mRecord;
    std::atomic<CuckooTable*> mHashTable;
    Log<OrderedLogImpl> mInsertLog;
    Log<OrderedLogImpl> mUpdateLog;
    std::atomic<PageList*> mPages;
public:
    class ScanProcessor {
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
        ScanQueryBatchProcessor query;
        const Record* record;
    private: // calculated members
        void next();

        void setCurrentEntry();

        Page::Iterator pageIter;
        Page::Iterator pageEnd;
        uint64_t currKey;
        CDMRecord::VersionIterator currVersionIter;
    public:
        ScanProcessor(const std::shared_ptr<crossbow::allocator>& alloc,
                 const PageList* pages,
                 size_t pageIdx,
                 size_t pageEndIdx,
                 const LogIterator& logIter,
                 const LogIterator& logEnd,
                 PageManager* pageManager,
                 const char* queryBuffer,
                 const std::vector<ScanQuery*>& queryData,
                 const Record* record);

        void process();
    };
    Table(PageManager& pageManager, const Schema& schema, uint64_t idx);

    ~Table();

    const Record& record() const {
        return mRecord;
    }

    const Schema& schema() const {
        return mRecord.schema();
    }

    TableType type() const {
        return mRecord.schema().type();
    }

    bool get(uint64_t key,
             size_t& size,
             const char*& data,
             const commitmanager::SnapshotDescriptor& snapshot,
             uint64_t& version,
             bool& isNewest) const;

    void insert(uint64_t key,
                size_t size,
                const char* const data,
                const commitmanager::SnapshotDescriptor& snapshot,
                bool* succeeded = nullptr);

    bool update(uint64_t key,
                size_t size,
                const char* const data,
                const commitmanager::SnapshotDescriptor& snapshot);

    bool remove(uint64_t key,
                const commitmanager::SnapshotDescriptor& snapshot);

    bool revert(uint64_t key,
                const commitmanager::SnapshotDescriptor& snapshot);

    void runGC(uint64_t minVersion);

    std::vector<ScanProcessor> startScan(int numThreads, const char* queryBuffer,
            const std::vector<ScanQuery*>& queries) const;
private:
    template<class Fun>
    bool genericUpdate(const Fun& appendFun,
                       uint64_t key,
                       const commitmanager::SnapshotDescriptor& snapshot);
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
    PageManager::Ptr mPageManager;
    GC gc;
    VersionManager mVersionManager;
    TableManager<Table, GC> tableManager;

    StoreImpl(const StorageConfig& config);

    StoreImpl(const StorageConfig& config, size_t totalMem);

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
};
} // namespace store
} // namespace tell
