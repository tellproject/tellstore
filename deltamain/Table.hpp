#pragma once

#include <util/CuckooHash.hpp>
#include <util/Log.hpp>

#include <tellstore/Record.hpp>

#include <crossbow/allocator.hpp>

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

class PageManager;
class ScanQuery;

namespace deltamain {

class Table {
    using PageList = std::vector<char*>;
    PageManager& mPageManager;
    Record mRecord;
    std::atomic<CuckooTable*> mHashTable;
    Log<OrderedLogImpl> mInsertLog;
    Log<OrderedLogImpl> mUpdateLog;
    std::atomic<PageList*> mPages;
#if defined USE_COLUMN_MAP
    const uint32_t mNumberOfFixedSizedFields;
    const uint32_t mNumberOfVarSizedFields;
#endif
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

#if defined USE_COLUMN_MAP
    const PageManager* pageManager() const {
        return pageManager();
    }

    const int32_t getFieldOffset(const Record::id_t id) const {
        return mRecord.getFieldMeta(id).second;
    }

    /**
     * assumes var-sized fields are constant size 8 (offset + prefix)
     */
    const size_t getFieldSize(const Record::id_t id) const {
        return id < mNumberOfFixedSizedFields ? mRecord.schema().fixedSizeFields().at(id).defaultSize() : 8;
    }

    const int32_t getNumberOfFixedSizedFields() const {
        return mNumberOfFixedSizedFields;
    }

    const int32_t getNumberOfVarSizedFields() const {
        return mNumberOfVarSizedFields;
    }
#endif

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
} // namespace store
} // namespace tell
