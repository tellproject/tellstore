#pragma once

#include <config.h>
#include <util/Log.hpp>
#include <util/OpenAddressingHash.hpp>
#include <util/PageManager.hpp>
#include <util/ScanQuery.hpp>
#include <util/StoreImpl.hpp>
#include <util/TableManager.hpp>
#include <util/VersionManager.hpp>

#include <tellstore/Record.hpp>

#include <crossbow/non_copyable.hpp>

#include <cstdint>

namespace tell {
namespace commitmanager {
class SnapshotDescriptor;
} // namespace commitmanager

namespace store {
namespace logstructured {

class ChainedVersionRecord;
class Table;
class VersionRecordIterator;

using LogImpl = Log<UnorderedLogImpl>;

/**
 * @brief Scan processor for the Log-Structured Memory approach that performs Garbage Collection as part of its scan
 */
class GcScanProcessor : crossbow::non_copyable {
public:
    GcScanProcessor(Table& table, const LogImpl::PageIterator& begin, const LogImpl::PageIterator& end,
            const char* queryBuffer, const std::vector<ScanQuery*>& queryData, uint64_t minVersion);

    GcScanProcessor(GcScanProcessor&& other);

    /**
     * @brief Scans over all entries in the log
     *
     * Processes all valid entries with the associated scan queries.
     *
     * Performs garbage collection while scanning over a page.
     */
    void process();

private:
    /**
     * @brief Advance the entry iterator to the next entry, advancing to the next page if necessary
     *
     * The processor must not be at the end when calling this function.
     */
    bool advanceEntry();

    /**
     * @brief Recycle the given element
     *
     * Copies the element to a recycling page and replaces the old element in the version list with the new element.
     *
     * @param oldElement The element to recycle
     * @param size Size of the old element
     * @param type Type of the old element
     */
    void recycleEntry(ChainedVersionRecord* oldElement, uint32_t size, uint32_t type);

    /**
     * @brief Replaces the given old element with the given new element in the version list of the record
     *
     * The two elements must belong to the same record.
     *
     * @param oldElement Old element to remove from the version list
     * @param newElement New element to replace the old element with
     * @return Whether the replacement was successful
     */
    bool replaceElement(ChainedVersionRecord* oldElement, ChainedVersionRecord* newElement);

    Table& mTable;
    ScanQueryBatchProcessor mQueries;
    uint64_t mMinVersion;

    LogImpl::PageIterator mPagePrev;
    LogImpl::PageIterator mPageIt;
    LogImpl::PageIterator mPageEnd;

    LogPage::EntryIterator mEntryIt;
    LogPage::EntryIterator mEntryEnd;

    LogPage* mRecyclingHead;
    LogPage* mRecyclingTail;

    /// Amount of garbage in the current page
    uint32_t mGarbage;

    /// Whether all entries in the current page were sealed
    bool mSealed;

    /// Whether the current page is being recycled
    /// Initialized to false to prevent the first page from being garbage collected
    bool mRecycle;
};

/**
 * @brief A table using a Log-Structured Memory approach as its data store
 */
class Table : crossbow::non_copyable, crossbow::non_movable {
public:
    using HashTable = OpenAddressingTable;

    using ScanProcessor = GcScanProcessor;

    Table(PageManager& pageManager, const Schema& schema, uint64_t tableId, VersionManager& versionManager,
            HashTable& hashMap);

    uint64_t id() const {
        return mTableId;
    }

    const Record& record() const {
        return mRecord;
    }

    const Schema& schema() const {
        return mRecord.schema();
    }

    /**
     * @brief Reads a tuple from the table
     *
     * @param key Key of the tuple to retrieve
     * @param size Reference to the tuple's size
     * @param data Reference to the tuple's data pointer
     * @param snapshot Descriptor containing the versions allowed to read
     * @param version Reference to the tuple's version
     * @param isNewest Whether the returned tuple contains the newest version written
     * @return Whether the tuple was found
     */
    bool get(uint64_t key, size_t& size, const char*& data, const commitmanager::SnapshotDescriptor& snapshot,
            uint64_t& version, bool& isNewest);

    /**
     * @brief Inserts a tuple into the table
     *
     * @param key Key of the tuple to insert
     * @param size Size of the tuple to insert
     * @param data Pointer to the data of the tuple to insert
     * @param snapshot Descriptor containing the version to write
     * @param succeeded Whether the tuple was inserted successfully
     */
    void insert(uint64_t key, size_t size, const char* data, const commitmanager::SnapshotDescriptor& snapshot,
            bool* succeeded = nullptr);

    /**
     * @brief Updates an already existing tuple in the table
     *
     * @param key Key of the tuple to update
     * @param size Size of the updated tuple
     * @param data Pointer to the data of the updated tuple
     * @param snapshot Descriptor containing the version to write
     * @return Whether the tuple was updated successfully
     */
    bool update(uint64_t key, size_t size, const char* data, const commitmanager::SnapshotDescriptor& snapshot);

    /**
     * @brief Removes an already existing tuple from the table
     *
     * @param key Key of the tuple to remove
     * @param snapshot Descriptor containing the version to remove
     * @return Whether the tuple was removed successfully
     */
    bool remove(uint64_t key, const commitmanager::SnapshotDescriptor& snapshot);

    /**
     * @brief Reverts the existing element with the given version to the element with the previous version
     *
     * At this time only the element with the most recent version can be reverted.
     *
     * @param key Key of the tuple to revert
     * @param snapshot Descriptor containing the version to revert
     * @return Whether the element was successfully reverted to the older version
     */
    bool revert(uint64_t key, const commitmanager::SnapshotDescriptor& snapshot);

    /**
     * @brief Start a full scan of this table
     *
     * @param numThreads Number of threads to use for the scan
     * @param queryBuffer The query buffer containing the combined selection buffer of all attached queries
     * @param queries Queries attaching to this scan
     * @return A scan processor for each thread
     */
    std::vector<ScanProcessor> startScan(int numThreads, const char* queryBuffer,
            const std::vector<ScanQuery*>& queries);

    /**
     * @brief Starts a garbage collection run
     *
     * @param minVersion Minimum version of the tuples to keep
     */
    void runGC(uint64_t minVersion);

private:
    friend class GcScanProcessor;
    friend class LazyRecordWriter;
    friend class VersionRecordIterator;

    /**
     * @brief The lowest active version of the tuples in this table
     */
    uint64_t minVersion() const;

    /**
     * @brief Finds the key in the table
     *
     * @param key Key of the record to lookup
     */
    VersionRecordIterator find(uint64_t key);

    /**
     * @brief Helper function to write a update or a deletion entry
     *
     * @param key Key of the entry to write
     * @param size Size of the data to write
     * @param data Pointer to the data to write
     * @param snapshot Descriptor containing the version to write
     * @param deleted Whether the entry marks a deletion
     * @return Whether the entry was successfully written
     */
    bool internalUpdate(uint64_t key, size_t size, const char* data, const commitmanager::SnapshotDescriptor& snapshot,
            bool deletion);

    VersionManager& mVersionManager;
    HashTable& mHashMap;
    Record mRecord;
    const uint64_t mTableId;

    LogImpl mLog;
};

/**
 * @brief Garbage collector to reclaim unused pages in the Log-Structured Memory approach
 */
class GarbageCollector {
public:
    void run(const std::vector<Table*>& tables, uint64_t minVersion);
};

} // namespace logstructured

/**
 * @brief A Storage implementation using a Log-Structured Memory approach as its data store
 */
template<>
struct StoreImpl<Implementation::LOGSTRUCTURED_MEMORY> : crossbow::non_copyable, crossbow::non_movable {
public:
    using Table = logstructured::Table;
    using GC = logstructured::GarbageCollector;
    using StorageType = StoreImpl<Implementation::LOGSTRUCTURED_MEMORY>;

    StoreImpl(const StorageConfig& config);

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
