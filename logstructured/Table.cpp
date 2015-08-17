#include "Table.hpp"

#include "ChainedVersionRecord.hpp"
#include "VersionRecordIterator.hpp"

#include <util/VersionManager.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/enum_underlying.hpp>
#include <crossbow/logger.hpp>

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
    entry->seal();
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
    entry->seal();
    mRecord = nullptr;
}

Table::Table(PageManager& pageManager, const Schema& schema, uint64_t tableId, VersionManager& versionManager,
        HashTable& hashMap)
        : mVersionManager(versionManager),
          mHashMap(hashMap),
          mRecord(schema),
          mTableId(tableId),
          mLog(pageManager) {
}

bool Table::get(uint64_t key, size_t& size, const char*& data, const commitmanager::SnapshotDescriptor& snapshot,
        uint64_t& version, bool& isNewest) {
    auto recIter = find(key);
    for (; !recIter.done(); recIter.next()) {
        if (!snapshot.inReadSet(recIter->validFrom())) {
            continue;
        }
        auto entry = LogEntry::entryFromData(reinterpret_cast<const char*>(recIter.value()));

        // Set the version and isNewest field
        version = recIter->validFrom();
        isNewest = recIter.isNewest();

        // Check if the entry marks a deletion
        if (crossbow::from_underlying<VersionRecordType>(entry->type()) == VersionRecordType::DELETION) {
            return false;
        }

        // Set the data pointer and size field
        data = recIter->data();
        size = entry->size() - sizeof(ChainedVersionRecord);
        return true;
    }

    // Element not found
    isNewest = recIter.isNewest();
    return false;
}

void Table::insert(uint64_t key, size_t size, const char* data, const commitmanager::SnapshotDescriptor& snapshot,
        bool* succeeded /* = nullptr */) {
    LazyRecordWriter recordWriter(*this, key, data, size, VersionRecordType::DATA, snapshot.version());
    auto recIter = find(key);
    LOG_ASSERT(mRecord.schema().type() == TableType::NON_TRANSACTIONAL || snapshot.version() >= recIter.minVersion(),
            "Version of the snapshot already committed");

    while (true) {
        LOG_ASSERT(recIter.isNewest(), "Version iterator must point to newest version");

        bool sameVersion;
        if (!recIter.done()) {
            // Cancel if element is not in the read set
            if (!snapshot.inReadSet(recIter->validFrom())) {
                break;
            }

            auto oldEntry = LogEntry::entryFromData(reinterpret_cast<const char*>(recIter.value()));

            // Check if the entry marks a data tuple
            if (crossbow::from_underlying<VersionRecordType>(oldEntry->type()) == VersionRecordType::DATA) {
                break;
            }

            // Cancel if a concurrent revert is taking place
            if (recIter.validTo() != ChainedVersionRecord::ACTIVE_VERSION) {
                break;
            }

            // Cancel if the entry is not yet sealed
            if (BOOST_UNLIKELY(!oldEntry->sealed())) {
                break;
            }

            sameVersion = (recIter->validFrom() == snapshot.version());
        } else {
            sameVersion = false;
        }

        auto record = recordWriter.record();
        if (!record) {
            break;
        }

        auto res = (sameVersion ? recIter.replace(record)
                                : recIter.insert(record));
        if (!res) {
            continue;
        }
        recordWriter.seal();

        if (succeeded) *succeeded = true;
        return;
    }

    if (succeeded) *succeeded = false;
}

bool Table::update(uint64_t key, size_t size, const char* data, const commitmanager::SnapshotDescriptor& snapshot) {
    return internalUpdate(key, size, data, snapshot, false);
}

bool Table::remove(uint64_t key, const commitmanager::SnapshotDescriptor& snapshot) {
    return internalUpdate(key, 0, nullptr, snapshot, true);
}

bool Table::revert(uint64_t key, const commitmanager::SnapshotDescriptor& snapshot) {
    auto recIter = find(key);
    while (!recIter.done()) {
        // Cancel if the element in the hash map is of a different version
        if (recIter->validFrom() != snapshot.version()) {
            // Succeed if the version list contains no element of the given version
            return !recIter.find(snapshot.version());
        }

        // Cancel if a concurrent revert is taking place
        if (recIter.validTo() != ChainedVersionRecord::ACTIVE_VERSION) {
            return false;
        }

        // Cancel if the entry is not yet sealed
        // At the moment this is only the case when the element was inserted successfully into the version list but the
        // validTo version of the next element was not yet updated. We have to abort the revert to prevent a race
        // condition where the running write sets the validTo version to expired after we reverted the element (even
        // though it should be active).
        auto entry = LogEntry::entryFromData(reinterpret_cast<const char*>(recIter.value()));
        if (BOOST_UNLIKELY(!entry->sealed())) {
            return false;
        }

        // This only fails when a newer element was written in the meantime or the garbage collection recycled the
        // current element
        if (recIter.remove()) {
            return true;
        }
    }

    // Element not found
    return false;
}

std::vector<Table::ScanProcessor> Table::startScan(size_t numThreads, const char* queryBuffer,
        const std::vector<ScanQuery*>& queries) {
    std::vector<ScanProcessor> result;
    result.reserve(numThreads);

    auto version = minVersion();
    auto numPages = mLog.pages();
    auto begin = mLog.pageBegin();
    auto end = mLog.pageEnd();

    auto mod = numPages % numThreads;
    auto iter = begin;
    for (decltype(numThreads) i = 1; i < numThreads; ++i) {
        auto step = numPages / numThreads + (i < mod ? 1 : 0);
        // Increment the page iterator by step pages (but not beyond the end page)
        for (decltype(step) j = 0; j < step && iter != end; ++j, ++iter) {
        }

        result.emplace_back(*this, begin, iter, queryBuffer, queries, version);
        begin = iter;
    }

    // The last scan takes the remaining pages
    result.emplace_back(*this, begin, end, queryBuffer, queries, version);

    return result;
}

void Table::runGC(uint64_t minVersion) {
    // TODO Implement
}

uint64_t Table::minVersion() const {
    if (mRecord.schema().type() == TableType::NON_TRANSACTIONAL) {
        return ChainedVersionRecord::ACTIVE_VERSION;
    } else {
        return mVersionManager.lowestActiveVersion();
    }
}

VersionRecordIterator Table::find(uint64_t key) {
    return VersionRecordIterator(*this, minVersion(), key);
}

bool Table::internalUpdate(uint64_t key, size_t size, const char* data,
        const commitmanager::SnapshotDescriptor& snapshot, bool deletion) {
    auto type = (deletion ? VersionRecordType::DELETION : VersionRecordType::DATA);
    LazyRecordWriter recordWriter(*this, key, data, size, type, snapshot.version());
    auto recIter = find(key);
    LOG_ASSERT(mRecord.schema().type() == TableType::NON_TRANSACTIONAL || snapshot.version() >= recIter.minVersion(),
            "Version of the snapshot already committed");

    while (!recIter.done()) {
        LOG_ASSERT(recIter.isNewest(), "Version iterator must point to newest version");

        // Cancel if element is not in the read set
        if (!snapshot.inReadSet(recIter->validFrom())) {
            break;
        }

        auto oldEntry = LogEntry::entryFromData(reinterpret_cast<const char*>(recIter.value()));

        // Check if the entry marks a deletion
        if (crossbow::from_underlying<VersionRecordType>(oldEntry->type()) == VersionRecordType::DELETION) {
            break;
        }

        // Cancel if a concurrent revert is taking place
        if (recIter.validTo() != ChainedVersionRecord::ACTIVE_VERSION) {
            break;
        }

        // Cancel if the entry is not yet sealed
        if (BOOST_UNLIKELY(!oldEntry->sealed())) {
            break;
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
            return true;
        }

        auto record = recordWriter.record();
        if (!record) {
            break;
        }

        auto res = (sameVersion ? recIter.replace(record)
                                : recIter.insert(record));
        if (!res) {
            continue;
        }
        recordWriter.seal();

        return true;
    }

    return false;
}

void GarbageCollector::run(const std::vector<Table*>& tables, uint64_t minVersion) {
    // TODO Implement
}

} // namespace logstructured
} // namespace store
} // namespace tell
