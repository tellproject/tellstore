#pragma once

#include <config.h>

#if defined USE_LOG_SCAN
#include "GcScanProcessor.hpp"
#elif defined USE_HASH_SCAN
#include "HashScanProcessor.hpp"
#else
#error "Unknown scan processor"
#endif

#include <util/Log.hpp>
#include <util/OpenAddressingHash.hpp>

#include <tellstore/Record.hpp>

#include <crossbow/non_copyable.hpp>

#include <cstdint>

namespace tell {
namespace commitmanager {
class SnapshotDescriptor;
} // namespace commitmanager

namespace store {

class PageManager;
class ScanQuery;
class VersionManager;

namespace logstructured {

class VersionRecordIterator;

/**
 * @brief A table using a Log-Structured Memory approach as its data store
 */
class Table : crossbow::non_copyable, crossbow::non_movable {
public:
    using HashTable = OpenAddressingTable;

    using LogImpl = Log<UnorderedLogImpl>;

#if defined USE_LOG_SCAN
    using ScanProcessor = GcScanProcessor;
#elif defined USE_HASH_SCAN
    using ScanProcessor = HashScanProcessor;
#else
#error "Unknown scan processor"
#endif

    using GarbageCollector = ScanProcessor::GarbageCollector;

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
    std::vector<ScanProcessor> startScan(size_t numThreads, const char* queryBuffer,
            const std::vector<ScanQuery*>& queries) {
        return ScanProcessor::startScan(*this, numThreads, queryBuffer, queries);
    }

private:
    friend class GcScanProcessor;
    friend class HashScanProcessor;
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

} // namespace logstructured
} // namespace store
} // namespace tell
