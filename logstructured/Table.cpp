#include "Table.hpp"

#include "Record.hpp"

#include <util/OpenAddressingHash.hpp>
#include <util/PageManager.hpp>

namespace tell {
namespace store {
namespace logstructured {

namespace {

/**
 * @brief Size of the LogRecord headers
 */
constexpr size_t gRecordHeaderSize = sizeof(LSMRecord) + sizeof(ChainedVersionRecord);

#define checkKey(_key) if (_key == 0) {\
    LOG_ASSERT(_key == 0, "Key should never be 0");\
        return false;\
    }

/**
 * @brief Returns the record size contained in the LSM Record
 */
size_t recordSize(const LSMRecord* record) {
    auto entry = LogEntry::entryFromData(reinterpret_cast<const char*>(record));
    return (entry->size() - gRecordHeaderSize);
}

} // anonymous namespace

Table::Table(PageManager& pageManager, const Schema& schema, uint64_t tableId, HashTable& hashMap)
    : mPageManager(pageManager),
      mHashMap(hashMap),
      mRecord(schema),
      mTableId(tableId),
      mLog(mPageManager) {
}

bool Table::get(uint64_t key, size_t& size, const char*& data, const SnapshotDescriptor& snapshot, bool& isNewest)
        const {
    checkKey(key);

    auto versionRecord = reinterpret_cast<const ChainedVersionRecord*>(mHashMap.get(mTableId, key));
    if (!versionRecord) {
        isNewest = true;
        return false;
    }

    while (true) {
        auto lsmRecord = LSMRecord::recordFromData(reinterpret_cast<const char*>(versionRecord));
        LOG_ASSERT(lsmRecord->key() == key, "Hash table points to LSMRecord with wrong key");

        auto from = versionRecord->validFrom();

        // Check if the element was already invalidated - Retry from the beginning
        // This situation only occurs when a concurrent revert happened on the current element, the hash map will point
        // to a valid element again
        if (from == ChainedVersionRecord::INVALID_VERSION) {
            versionRecord = reinterpret_cast<const ChainedVersionRecord*>(mHashMap.get(mTableId, key));
            if (!versionRecord) {
                isNewest = true;
                return false;
            }
            continue;
        }

        // Check if the element is not in the read set
        if (!snapshot.inReadSet(from)) {
            // Check if we have a previous element
            versionRecord = versionRecord->getPrevious();
            if (versionRecord) {
                continue;
            }

            // Element not found (not in read set)
            isNewest = false;
            return false;
        }

        // Check if element is newest
        auto to = versionRecord->validTo();
        isNewest = (to == 0x0u);

        // This entry is either the newest or the newer one is not in the read set
        LOG_ASSERT(isNewest || !snapshot.inReadSet(to), "Trying to read an older entry when the newer entry is in the "
                "read set");

        // Check if the entry was deleted in this version
        if (versionRecord->wasDeleted()) {
            return false;
        }

        // Set the data pointer and size field
        data = versionRecord->data();
        size = recordSize(lsmRecord);

        return true;
    }
}

bool Table::getNewest(uint64_t key, size_t& size, const char*& data, uint64_t& version) const {
    checkKey(key);

    auto versionRecord = reinterpret_cast<const ChainedVersionRecord*>(mHashMap.get(mTableId, key));
    if (!versionRecord) {
        version = 0x0u;
        return false;
    }

    while (true) {
        auto lsmRecord = LSMRecord::recordFromData(reinterpret_cast<const char*>(versionRecord));
        LOG_ASSERT(lsmRecord->key() == key, "Hash table points to LSMRecord with wrong key");

        // Check if the element was not invalidated in the meantime
        version = versionRecord->validFrom();
        if (version == ChainedVersionRecord::INVALID_VERSION) {
            // Check if we have a previous element
            versionRecord = versionRecord->getPrevious();
            if (versionRecord) {
                continue;
            }
            version = 0x0u;
            return false;
        }

        // Check if the entry was deleted in this version
        if (versionRecord->wasDeleted()) {
            return false;
        }

        // Set the data pointer and size field
        data = versionRecord->data();
        size = recordSize(lsmRecord);

        return true;
    }
}

void Table::insert(uint64_t key, const GenericTuple& tuple, const SnapshotDescriptor& snapshot,
        bool* succeeded /* = nullptr */) {
    size_t size;
    std::unique_ptr<char[]> rec(mRecord.create(tuple, size));
    insert(key, size, rec.get(), snapshot, succeeded);
    // TODO This can be implemented faster by directly serializing the tuple to the log
}

void Table::insert(uint64_t key, size_t size, const char* data, const SnapshotDescriptor& snapshot,
        bool* succeeded /* = nullptr */) {
    if (key == 0) {
        LOG_ASSERT(key == 0, "Key should never be 0");
        if (succeeded) *succeeded = false;
        return;
    }

    auto prev = reinterpret_cast<ChainedVersionRecord*>(mHashMap.get(mTableId, key));
    if (prev) {
        // Entry already exists - Cancel if it is not in the read set or was not a delete
        if (!snapshot.inReadSet(prev->validFrom()) || !prev->wasDeleted()) {
            if (succeeded) *succeeded = false;
            return;
        }
    }

    // Write entry into log
    auto res = writeEntry(key, snapshot.version(), prev, size, data, false);
    if (succeeded) *succeeded = res;
}

bool Table::update(uint64_t key, size_t size, const char* data, const SnapshotDescriptor& snapshot) {
    checkKey(key);

    auto prev = reinterpret_cast<ChainedVersionRecord*>(mHashMap.get(mTableId, key));
    if (!prev) {
        return false;
    }

    // Cancel if entry is not in the read set or was a delete
    if (!snapshot.inReadSet(prev->validFrom()) || prev->wasDeleted()) {
        return false;
    }

    // Write entry into log
    return writeEntry(key, snapshot.version(), prev, size, data, false);
}

bool Table::remove(uint64_t key, const SnapshotDescriptor& snapshot) {
    checkKey(key);

    auto prev = reinterpret_cast<ChainedVersionRecord*>(mHashMap.get(mTableId, key));
    if (!prev) {
        return false;
    }

    // Cancel if entry is not in the read set or already was a delete
    if (!snapshot.inReadSet(prev->validFrom()) || prev->wasDeleted()) {
        return false;
    }

    // Write entry into log
    return writeEntry(key, snapshot.version(), prev, 0, nullptr, true);
}

bool Table::revert(uint64_t key, const SnapshotDescriptor& snapshot) {
    checkKey(key);

    auto versionRecord = reinterpret_cast<ChainedVersionRecord*>(mHashMap.get(mTableId, key));
    if (!versionRecord) {
        return true;
    }

    while (true) {
        auto lsmRecord = LSMRecord::recordFromData(reinterpret_cast<const char*>(versionRecord));
        LOG_ASSERT(lsmRecord->key() == key, "Hash table points to LSMRecord with wrong key");

        auto from = versionRecord->validFrom();

        // Check if the element was already invalidated - Retry from the beginning
        if (from == ChainedVersionRecord::INVALID_VERSION) {
            versionRecord = reinterpret_cast<ChainedVersionRecord*>(mHashMap.get(mTableId, key));
            if (!versionRecord) {
                return true;
            }
            continue;
        }

        // The element in the hash map is of a different version
        if (from != snapshot.version()) {
            // Return true if the element already is older (we do not have to revert anything), false when the element
            // is newer (this should in theory never happen)
            return (from < snapshot.version());
        }

        // If the element has no previous element we can delete it from the hash map
        auto prev = versionRecord->getPrevious();
        auto res = (prev ? mHashMap.update(mTableId, key, versionRecord, prev)
                         : mHashMap.erase(mTableId, key, versionRecord));

        // Check if updating the element into the hash table failed
        if (!res) {
            return false;
        }

        // Invalidate the element we just overrode
        invalidateTuple(versionRecord);

        if (prev) {
            // Reactivate the old element - We do not care if this fails as this only happens if another element with
            // same key was inserted and thus already updated the valid-to version
            prev->reactivate(snapshot.version());
        }

        return true;
    }
}

void Table::runGC(uint64_t minVersion) {
    // TODO Implement
}

bool Table::writeEntry(uint64_t key, uint64_t version, ChainedVersionRecord* prev, size_t size, const char* data,
        bool deleted) {
    // Check if the previous element was already set to expire in this version
    // This means that a concurrent revert with the same version is still running
    if (prev && prev->validTo() == version) {
        return false;
    }

    // Check if we are overriding an element with the same version
    // In this case we can set the new element's previous pointer to prev's previous pointer as the prev element will
    // never be read by any version
    auto sameVersion = (prev && prev->validFrom() == version);
    auto recordPrev = (sameVersion ? prev->getPrevious() : prev);

    // Check if we delete an element with the same version and the element has no ancestors
    // In this case we can simply remove the element from the hash map as the element will never be read by any version
    if (sameVersion && deleted && !recordPrev) {
        auto res = mHashMap.erase(mTableId, key, prev);
        if (res) {
            invalidateTuple(prev);
        }
        return res;
    }

    // Append the entry in the log
    auto entry = mLog.append(size + gRecordHeaderSize);
    if (!entry) {
        LOG_FATAL("Failed to append to log");
        return false;
    }

    // Write entry to log
    auto lsmRecord = new (entry->data()) LSMRecord(key);
    auto versionRecord = new (lsmRecord->data()) ChainedVersionRecord(version, recordPrev, deleted);
    memcpy(versionRecord->data(), data, size);
    entry->seal();

    // Insert or update the element in the hash table (depending if there is a valid prev pointer)
    auto res = (prev ? mHashMap.update(mTableId, key, prev, versionRecord)
                     : mHashMap.insert(mTableId, key, versionRecord));

    // Check if updating the element into the hash table failed
    if (!res) {
        invalidateTuple(versionRecord);
        return false;
    }

    if (sameVersion) {
        // Invalidate the element we just overrode
        invalidateTuple(prev);
    } else if (prev) {
        // Let the previous element expire in this version
        prev->expire(version);
    }

    return true;
}

void Table::invalidateTuple(ChainedVersionRecord* versionRecord) {
    versionRecord->invalidate();
    // TODO Inform replication that the tuple is invalid (might need to write a Tombstone)
}

void GarbageCollector::run(const std::vector<Table*>& tables, uint64_t minVersion) {
    // TODO Implement
}

} // namespace logstructured

StoreImpl<Implementation::LOGSTRUCTURED_MEMORY>::StoreImpl(const StorageConfig& config, size_t totalMem)
        : mPageManager(totalMem),
          mTableManager(mPageManager, config, mGc, mCommitManager),
          mHashMap(config.hashMapCapacity) {
}

} // namespace store
} // namespace tell
