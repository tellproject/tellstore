/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#pragma once

#include <config.h>

#if defined USE_LOG_SCAN
#include "GcScanProcessor.hpp"
#elif defined USE_HASH_SCAN
#include "HashScanProcessor.hpp"
#else
#error "Unknown scan processor"
#endif

#include "ChainedVersionRecord.hpp"
#include "VersionRecordIterator.hpp"

#include <util/Log.hpp>
#include <util/OpenAddressingHash.hpp>

#include <tellstore/ErrorCode.hpp>
#include <tellstore/Record.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/enum_underlying.hpp>
#include <crossbow/non_copyable.hpp>

#include <cstdint>

namespace tell {
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
    using Scan = GcScan;
#elif defined USE_HASH_SCAN
    using Scan = HashScan;
#else
#error "Unknown scan processor"
#endif

    using ScanProcessor = Scan::ScanProcessor;
    using GarbageCollector = Scan::GarbageCollector;

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
     * @param snapshot Descriptor containing the versions allowed to read
     * @param fun The materilization function taking the size, version and whether the tuple is the newest one and
     *   returning a pointer where the result will be written
     * @return Error code or 0 if the tuple was successfully retrieved
     */
    template <typename Fun>
    int get(uint64_t key, const commitmanager::SnapshotDescriptor& snapshot, Fun fun);

    /**
     * @brief Inserts a tuple into the table
     *
     * @param key Key of the tuple to insert
     * @param size Size of the tuple to insert
     * @param data Pointer to the data of the tuple to insert
     * @param snapshot Descriptor containing the version to write
     * @return Error code or 0 if the tuple was successfully inserted
     */
    int insert(uint64_t key, size_t size, const char* data, const commitmanager::SnapshotDescriptor& snapshot);

    /**
     * @brief Updates an already existing tuple in the table
     *
     * @param key Key of the tuple to update
     * @param size Size of the updated tuple
     * @param data Pointer to the data of the updated tuple
     * @param snapshot Descriptor containing the version to write
     * @return Error code or 0 if the tuple was successfully updated
     */
    int update(uint64_t key, size_t size, const char* data, const commitmanager::SnapshotDescriptor& snapshot);

    /**
     * @brief Removes an already existing tuple from the table
     *
     * @param key Key of the tuple to remove
     * @param snapshot Descriptor containing the version to remove
     * @return Error code or 0 if the tuple was successfully removed
     */
    int remove(uint64_t key, const commitmanager::SnapshotDescriptor& snapshot);

    /**
     * @brief Reverts the existing element with the given version to the element with the previous version
     *
     * At this time only the element with the most recent version can be reverted.
     *
     * @param key Key of the tuple to revert
     * @param snapshot Descriptor containing the version to revert
     * @return Error code or 0 if the tuple was successfully reverted
     */
    int revert(uint64_t key, const commitmanager::SnapshotDescriptor& snapshot);

private:
    friend class GcScan;
    friend class GcScanProcessor;
    friend class HashScan;
    friend class HashScanProcessor;
    friend class LazyRecordWriter;
    friend class VersionRecordIterator;

    /**
     * @brief The lowest active version of the tuples in this table
     */
    uint64_t minVersion() const;

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
    int internalUpdate(uint64_t key, size_t size, const char* data, const commitmanager::SnapshotDescriptor& snapshot,
            bool deletion);

    VersionManager& mVersionManager;
    HashTable& mHashMap;
    Record mRecord;
    const uint64_t mTableId;

    LogImpl mLog;
};

template <typename Fun>
int Table::get(uint64_t key, const commitmanager::SnapshotDescriptor& snapshot, Fun fun) {
    VersionRecordIterator recIter(*this, key);
    for (; !recIter.done(); recIter.next()) {
        if (!snapshot.inReadSet(recIter->validFrom())) {
            continue;
        }
        auto entry = LogEntry::entryFromData(reinterpret_cast<const char*>(recIter.value()));

        // Check if the entry marks a deletion
        if (entry->type() == crossbow::to_underlying(VersionRecordType::DELETION)) {
            return (recIter.isNewest() ? error::not_found : error::not_in_snapshot);
        }

        auto size = entry->size() - sizeof(ChainedVersionRecord);
        auto dest = fun(size, recIter->validFrom(), recIter.isNewest());
        memcpy(dest, recIter->data(), size);
        return 0;
    }

    // Element not found
    return (recIter.isNewest() ? error::not_found : error::not_in_snapshot);
}

} // namespace logstructured
} // namespace store
} // namespace tell
