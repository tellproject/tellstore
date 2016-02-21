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

#include "Table.hpp"

#include <util/VersionManager.hpp>

#include <boost/config.hpp>

namespace tell {
namespace store {
namespace logstructured {

/**
 * @brief Helper class used to write a record to the log
 *
 * Implements the RAII pattern for writing a log record, ensures that the object is either successfully written and
 * sealed (by calling LazyRecordWriter::seal()) or the log record is invalidated and sealed when the writer object goes
 * out of scope.
 */
class LazyRecordWriter {
public:
    LazyRecordWriter(Table& table, uint64_t key, const char* data, uint32_t size, VersionRecordType type,
            uint64_t version)
            : mTable(table),
              mKey(key),
              mData(data),
              mSize(size),
              mType(type),
              mVersion(version),
              mRecord(nullptr) {
    }

    ~LazyRecordWriter();

    /**
     * @brief The pointer to the record in the log
     *
     * Writes the record in a lazy manner the first time this function is called.
     */
    inline ChainedVersionRecord* record();

    /**
     * @brief Seals the record in the log
     */
    inline void seal();

private:
    Table& mTable;
    uint64_t mKey;
    const char* mData;
    uint32_t mSize;
    VersionRecordType mType;
    uint64_t mVersion;
    ChainedVersionRecord* mRecord;
};

LazyRecordWriter::~LazyRecordWriter() {
    if (!mRecord) {
        return;
    }

    mRecord->invalidate();
    auto entry = LogEntry::entryFromData(reinterpret_cast<char*>(mRecord));
    mTable.mLog.seal(entry);
}

ChainedVersionRecord* LazyRecordWriter::record() {
    if (mRecord) {
        return mRecord;
    }

    auto entry = mTable.mLog.append(mSize + sizeof(ChainedVersionRecord), crossbow::to_underlying(mType));
    if (!entry) {
        LOG_FATAL("Failed to append to log");
        return nullptr;
    }

    // Write entry to log
    mRecord = new (entry->data()) ChainedVersionRecord(mKey, mVersion);
    memcpy(mRecord->data(), mData, mSize);

    return mRecord;
}

void LazyRecordWriter::seal() {
    if (!mRecord) {
        return;
    }

    auto entry = LogEntry::entryFromData(reinterpret_cast<char*>(mRecord));
    mTable.mLog.seal(entry);
    mRecord = nullptr;
}

Table::Table(MemoryReclaimer& memoryManager, PageManager& pageManager, const crossbow::string& tableName,
        const Schema& schema, uint64_t tableId, VersionManager& versionManager, HashTable& hashMap)
        : mMemoryManager(memoryManager),
          mPageManager(pageManager),
          mVersionManager(versionManager),
          mHashMap(hashMap),
          mTableName(tableName),
          mRecord(schema),
          mTableId(tableId),
          mLog(mPageManager) {
}

int Table::insert(uint64_t key, size_t size, const char* data, const commitmanager::SnapshotDescriptor& snapshot) {
    LazyRecordWriter recordWriter(*this, key, data, size, VersionRecordType::DATA, snapshot.version());
    VersionRecordIterator recIter(*this, key);
    LOG_ASSERT(mRecord.schema().type() == TableType::NON_TRANSACTIONAL || snapshot.version() >= minVersion(),
            "Version of the snapshot already committed");

    while (true) {
        LOG_ASSERT(recIter.isNewest(), "Version iterator must point to newest version");

        bool sameVersion;
        if (!recIter.done()) {
            // Cancel if element is not in the read set
            if (!snapshot.inReadSet(recIter->validFrom())) {
                return error::not_in_snapshot;
            }

            auto oldEntry = LogEntry::entryFromData(reinterpret_cast<const char*>(recIter.value()));

            // Check if the entry marks a data tuple
            if (crossbow::from_underlying<VersionRecordType>(oldEntry->type()) == VersionRecordType::DATA) {
                return error::invalid_write;
            }

            // Cancel if a concurrent revert is taking place
            if (recIter.validTo() != ChainedVersionRecord::ACTIVE_VERSION) {
                return error::not_in_snapshot;
            }

            // Cancel if the entry is not yet sealed
            if (BOOST_UNLIKELY(!oldEntry->sealed())) {
                return error::not_in_snapshot;
            }

            sameVersion = (recIter->validFrom() == snapshot.version());
        } else {
            sameVersion = false;
        }

        auto record = recordWriter.record();
        if (!record) {
            return error::out_of_memory;
        }

        auto res = (sameVersion ? recIter.replace(record)
                                : recIter.insert(record));
        if (!res) {
            continue;
        }
        recordWriter.seal();

        return 0;
    }

    LOG_ASSERT(false, "Must never reach this point");
}

int Table::update(uint64_t key, size_t size, const char* data, const commitmanager::SnapshotDescriptor& snapshot) {
    return internalUpdate(key, size, data, snapshot, false);
}

int Table::remove(uint64_t key, const commitmanager::SnapshotDescriptor& snapshot) {
    return internalUpdate(key, 0, nullptr, snapshot, true);
}

int Table::revert(uint64_t key, const commitmanager::SnapshotDescriptor& snapshot) {
    VersionRecordIterator recIter(*this, key);
    while (!recIter.done()) {
        // Cancel if the element in the hash map is of a different version
        if (recIter->validFrom() != snapshot.version()) {
            // Succeed if the version list contains no element of the given version
            return (!recIter.find(snapshot.version()) ? 0 : error::not_in_snapshot);
        }

        // Cancel if a concurrent revert is taking place
        if (recIter.validTo() != ChainedVersionRecord::ACTIVE_VERSION) {
            return error::not_in_snapshot;
        }

        // Cancel if the entry is not yet sealed
        // At the moment this is only the case when the element was inserted successfully into the version list but the
        // validTo version of the next element was not yet updated. We have to abort the revert to prevent a race
        // condition where the running write sets the validTo version to expired after we reverted the element (even
        // though it should be active).
        auto entry = LogEntry::entryFromData(reinterpret_cast<const char*>(recIter.value()));
        if (BOOST_UNLIKELY(!entry->sealed())) {
            return error::not_in_snapshot;
        }

        // This only fails when a newer element was written in the meantime or the garbage collection recycled the
        // current element
        if (recIter.remove()) {
            return 0;
        }
    }

    // Element not found
    return 0;
}

uint64_t Table::minVersion() const {
    if (mRecord.schema().type() == TableType::NON_TRANSACTIONAL) {
        return ChainedVersionRecord::ACTIVE_VERSION - 0x1u;
    } else {
        return mVersionManager.lowestActiveVersion();
    }
}

int Table::internalUpdate(uint64_t key, size_t size, const char* data,
        const commitmanager::SnapshotDescriptor& snapshot, bool deletion) {
    auto type = (deletion ? VersionRecordType::DELETION : VersionRecordType::DATA);
    LazyRecordWriter recordWriter(*this, key, data, size, type, snapshot.version());
    VersionRecordIterator recIter(*this, key);
    LOG_ASSERT(mRecord.schema().type() == TableType::NON_TRANSACTIONAL || snapshot.version() >= minVersion(),
            "Version of the snapshot already committed");

    while (!recIter.done()) {
        LOG_ASSERT(recIter.isNewest(), "Version iterator must point to newest version");

        // Cancel if element is not in the read set
        if (!snapshot.inReadSet(recIter->validFrom())) {
            return error::not_in_snapshot;
        }

        auto oldEntry = LogEntry::entryFromData(reinterpret_cast<const char*>(recIter.value()));

        // Check if the entry marks a deletion
        if (crossbow::from_underlying<VersionRecordType>(oldEntry->type()) == VersionRecordType::DELETION) {
            return error::invalid_write;
        }

        // Cancel if a concurrent revert is taking place
        if (recIter.validTo() != ChainedVersionRecord::ACTIVE_VERSION) {
            return error::not_in_snapshot;
        }

        // Cancel if the entry is not yet sealed
        if (BOOST_UNLIKELY(!oldEntry->sealed())) {
            return error::not_in_snapshot;
        }

        auto sameVersion = (recIter->validFrom() == snapshot.version());

        // Check if we delete an element with the same version and the element has no ancestors
        // In this case we can simply remove the element from the hash map as the element will never be read by any
        // version.
        if (sameVersion && deletion && !recIter.peekNext()) {
            if (!recIter.remove()) {
                // Version list changed - This might be due to update, revert or garbage collection, just retry again
                continue;
            }
            return 0;
        }

        auto record = recordWriter.record();
        if (!record) {
            return error::out_of_memory;
        }

        auto res = (sameVersion ? recIter.replace(record)
                                : recIter.insert(record));
        if (!res) {
            continue;
        }
        recordWriter.seal();

        return 0;
    }

    // Element not found
    return error::invalid_write;
}

void Table::releasePages(const std::vector<void*>& pages) {
    if (pages.empty()) {
        return;
    }

    mMemoryManager.wait();
    mPageManager.free(pages);
}

} // namespace logstructured
} // namespace store
} // namespace tell
