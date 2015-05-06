#pragma once

#include <config.h>
#include <implementation.hpp>
#include <util/CommitManager.hpp>
#include <util/Log.hpp>
#include <util/NonCopyable.hpp>
#include <util/OpenAddressingHash.hpp>
#include <util/Record.hpp>
#include <util/TableManager.hpp>
#include <util/TransactionImpl.hpp>

#include <cstdint>

namespace tell {
namespace store {

namespace logstructured {

class ChainedVersionRecord;

/**
 * @brief A table using a Log-Structured Memory approach as its data store
 */
class Table : NonCopyable, NonMovable {
public:
    using HashTable = OpenAddressingTable;

    Table(PageManager& pageManager, const Schema& schema, uint64_t tableId, HashTable& hashMap);

    /**
     * @brief Reads a tuple from the table
     *
     * @param key Key of the tuple to retrieve
     * @param size Reference to the tuple's size
     * @param data Reference to the tuple's data pointer
     * @param snapshot Descriptor containing the versions allowed to read
     * @param isNewest Whether the returned tuple contains the newest version written
     * @return Whether the tuple was found
     */
    bool get(uint64_t key, size_t& size, const char*& data, const SnapshotDescriptor& snapshot, bool& isNewest) const;

    /**
     * @brief Reads the newest tuple from the table
     *
     * @param key Key of the tuple to retrieve
     * @param size Reference to the tuple's size
     * @param data Reference to the tuple's data pointer
     * @param version Reference to the tuple's version
     * @return Whether the tuple was found
     */
    bool getNewest(uint64_t key, size_t& size, const char*& data, uint64_t& version) const;

    /**
     * @brief Inserts a tuple into the table
     *
     * @param key Key of the tuple to insert
     * @param tuple The tuple to insert
     * @param snapshot Descriptor containing the version to write
     * @param succeeded Whether the tuple was inserted successfully
     */
    void insert(uint64_t key, const GenericTuple& tuple, const SnapshotDescriptor& snapshot, bool* succeeded = nullptr);

    /**
     * @brief Inserts a tuple into the table
     *
     * @param key Key of the tuple to insert
     * @param size Size of the tuple to insert
     * @param data Pointer to the data of the tuple to insert
     * @param snapshot Descriptor containing the version to write
     * @param succeeded Whether the tuple was inserted successfully
     */
    void insert(uint64_t key, size_t size, const char* data, const SnapshotDescriptor& snapshot,
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
    bool update(uint64_t key, size_t size, const char* data, const SnapshotDescriptor& snapshot);

    /**
     * @brief Removes an already existing tuple from the table
     *
     * @param key Key of the tuple to remove
     * @param snapshot Descriptor containing the version to remove
     * @return Whether the tuple was removed successfully
     */
    bool remove(uint64_t key, const SnapshotDescriptor& snapshot);

    /**
     * @brief Reverts the existing element with the given version to the element with the previous version
     *
     * At this time only the element with the most recent version can be reverted.
     *
     * @param key Key of the tuple to revert
     * @param snapshot Descriptor containing the version to revert
     * @return Whether the element was successfully reverted to the older version
     */
    bool revert(uint64_t key, const SnapshotDescriptor& snapshot);

    /**
     * @brief Starts a garbage collection run
     *
     * @param minVersion Minimum version of the tuples to keep
     */
    void runGC(uint64_t minVersion);

private:
    /**
     * @brief Helper function to write a entry
     *
     * Writes the data to the log and updates the hash table. If the prev pointer is null the function tries to insert
     * the new entry into the hash table else the hash table is updated from the prev pointer to the new pointer.
     *
     * @param key Key of the entry to insert
     * @param version Version of the entry to insert
     * @param prev Pointer to previous version of the same key or null if no previous version exists
     * @param size Size of the data to write
     * @param data Pointer to the data to write
     * @param deleted Whether the entry marks a deletion
     * @return Whether the entry was successfully written
     */
    bool writeEntry(uint64_t key, uint64_t version, ChainedVersionRecord* prev, size_t size, const char* data,
            bool deleted);

    /**
     * @brief Marks the tuple as invalid in the log
     *
     * Invalid elements have no other elements or the hash map pointing to it and can be safely thrown away by the
     * garbage collector.
     */
    void invalidateTuple(ChainedVersionRecord* versionRecord);

    PageManager& mPageManager;
    HashTable& mHashMap;
    Record mRecord;
    uint64_t mTableId;

    Log<UnorderedLogImpl> mLog;
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
class StoreImpl<Implementation::LOGSTRUCTURED_MEMORY> : NonCopyable, NonMovable {
public:
    using Table = logstructured::Table;
    using GC = logstructured::GarbageCollector;
    using StorageType = StoreImpl<Implementation::LOGSTRUCTURED_MEMORY>;
    using Transaction = TransactionImpl<StorageType>;

    StoreImpl(const StorageConfig& config)
            : StoreImpl(config, config.totalMemory) {
    }

    StoreImpl(const StorageConfig& config, size_t totalMem);

    Transaction startTx() {
        return Transaction(*this, mCommitManager.startTx());
    }

    bool createTable(const crossbow::string& name, const Schema& schema, uint64_t& idx) {
        return mTableManager.createTable(name, schema, idx, mHashMap);
    }

    bool getTableId(const crossbow::string& name, uint64_t& id) {
        return mTableManager.getTableId(name, id);
    }

    bool get(uint64_t tableId, uint64_t key, size_t& size, const char*& data, const SnapshotDescriptor& snapshot,
            bool& isNewest) {
        return mTableManager.get(tableId, key, size, data, snapshot, isNewest);
    }

    bool update(uint64_t tableId, uint64_t key, size_t size, const char* data, const SnapshotDescriptor& snapshot) {
        return mTableManager.update(tableId, key, size, data, snapshot);
    }

    void insert(uint64_t tableId, uint64_t key, size_t size, const char* data, const SnapshotDescriptor& snapshot,
            bool* succeeded = nullptr) {
        mTableManager.insert(tableId, key, size, data, snapshot, succeeded);
    }

    void insert(uint64_t tableId, uint64_t key, const GenericTuple& tuple, const SnapshotDescriptor& snapshot,
            bool* succeeded = nullptr) {
        mTableManager.insert(tableId, key, tuple, snapshot, succeeded);
    }

    bool remove(uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot) {
        return mTableManager.remove(tableId, key, snapshot);
    }

    bool revert(uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot) {
        return mTableManager.revert(tableId, key, snapshot);
    }

    /**
     * We use this method mostly for test purposes. But
     * it might be handy in the future as well. If possible,
     * this should be implemented in an efficient way.
     */
    void forceGC() {
        mTableManager.forceGC();
    }

    void commit(SnapshotDescriptor& snapshot) {
        mCommitManager.commitTx(snapshot);
    }

    void abort(SnapshotDescriptor& snapshot) {
        // TODO: Roll-back. I am not sure whether this would generally
        // work. Probably not (since we might also need to roll back the
        // index which has to be done in the processing layer).
        mCommitManager.abortTx(snapshot);
    }

private:
    PageManager mPageManager;
    GC mGc;
    CommitManager mCommitManager;
    TableManager<Table, GC> mTableManager;

    Table::HashTable mHashMap;
};

} // namespace store
} // namespace tell
