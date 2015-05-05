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

Table::Table(PageManager& pageManager, HashTable& hashMap, const Schema& schema, uint64_t tableId)
    : mPageManager(pageManager),
      mHashMap(hashMap),
      mSchema(schema),
      mTableId(tableId),
      mLog(mPageManager) {
}

bool Table::get(uint64_t key, size_t& size, const char*& data, const SnapshotDescriptor& snapshot, bool& isNewest)
        const {
    checkKey(key);

    auto versionRecord = reinterpret_cast<const ChainedVersionRecord*>(mHashMap.get(mTableId, key));
    if (!versionRecord) {
        return false;
    }

    while (true) {
        auto lsmRecord = LSMRecord::recordFromData(reinterpret_cast<const char*>(versionRecord));
        LOG_ASSERT(lsmRecord->key() == key, "Hash table points to LSMRecord with wrong key");

        // Check if the element is not in the read set
        auto from = versionRecord->validFrom();
        if (!snapshot.inReadSet(from)) {
            // Check if we have a previous element
            versionRecord = versionRecord->getPrevious();
            if (versionRecord) {
                continue;
            }

            // Element not found (not in read set)
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
    // TODO Implement
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

void Table::runGC(uint64_t minVersion) {
    // TODO Implement
}

bool Table::writeEntry(uint64_t key, uint64_t version, ChainedVersionRecord* prev, size_t size, const char* data,
        bool deleted) {
    // Append the entry in the log
    auto entry = mLog.append(size + gRecordHeaderSize);
    if (!entry) {
        LOG_FATAL("Failed to append to log");
        return false;
    }

    // Write entry to log
    auto lsmRecord = new (entry->data()) LSMRecord(key);
    auto versionRecord = new (lsmRecord->data()) ChainedVersionRecord(version, prev, deleted);
    memcpy(versionRecord->data(), data, size);
    entry->seal();

    // Insert or update the element in the hash table (depending if there is a valid prev pointer)
    auto res = (prev ? mHashMap.update(mTableId, key, prev, versionRecord)
                     : mHashMap.insert(mTableId, key, versionRecord));

    // Check if updating the element into the hash table failed
    if (!res) {
        versionRecord->invalidate();
        return false;
    }

    // Let the previous element expire in this version
    if (prev) {
        prev->expire(version);
    }

    return true;
}

} // namespace logstructured
} // namespace store
} // namespace tell
