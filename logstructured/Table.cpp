#include "Table.hpp"

#include "Record.hpp"

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/enum_underlying.hpp>

#include <boost/config.hpp>

#include <tuple>

namespace tell {
namespace store {
namespace logstructured {

namespace {

/**
 * @brief Utilization threshold when to recycle a page in percent
 */
constexpr size_t gGcThreshold = 50;

} // anonymous namespace

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
    ChainedVersionRecord* record();

    /**
     * @brief Seals the record in the log
     */
    void seal();

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

/**
 * @brief Class to iterate over and manipulate the version list of a record
 *
 * Helps fixing the version list when encountering an invalid element.
 */
class VersionRecordIterator {
public:
    VersionRecordIterator(Table& table, uint64_t minVersion, uint64_t key)
            : mTable(table),
              mMinVersion(minVersion),
              mPrev(nullptr),
              mCurrent(retrieveHead(key)) {
        setCurrentEntry();
        LOG_ASSERT(isNewest(), "Start iterator not newest");
    }

    ChainedVersionRecord& operator*() const {
        return *operator->();
    }

    ChainedVersionRecord* operator->() const {
        return mCurrent;
    }

    uint64_t minVersion() const {
        return mMinVersion;
    }

    /**
     * @brief Whether the iterator points to the end of the list
     */
    bool done() const {
        return (mCurrent == nullptr);
    }

    /**
     * @brief Advance the iterator to the next element
     *
     * If the iterator encounters an invalid element it tries to fix the version list by removing the invalid element
     * from the list. The iterator will be transparently reset to the beginning in the case this fails and the iterator
     * can not determine the next element in the list reliably.
     *
     * Must not be called when already at the end.
     */
    void next() {
        LOG_ASSERT(!done(), "Trying to iterate over the end");

        mPrev = mCurrent;
        mPrevData = mCurrentData;
        mCurrent = peekNext();

        setCurrentEntry();
    }

    /**
     * @brief Whether the iterator points to the newest element in the version list
     */
    bool isNewest() const {
        return (mPrev == nullptr);
    }

    /**
     * @brief Return the next element in the version history without advancing the iterator
     *
     * Returns a null pointer in case the current element is the last one in the version history.
     *
     * Must not be called when already at the end.
     */
    ChainedVersionRecord* peekNext() {
        // If the current version is already before the min version then the next element is (or will be) invalid
        return (mCurrent->validFrom() < mMinVersion ? nullptr : mCurrentData.next());
    }

    /**
     * @brief The valid to version of the current element
     */
    uint64_t validTo() const {
        return mCurrentData.validTo();
    }

    /**
     * @brief The current element
     */
    ChainedVersionRecord* value() {
        return mCurrent;
    }

    /**
     * @brief Advances the iterator until it points to the given element
     *
     * In case the element is not found in the list the iterator will point to the end of the list or some arbitrary
     * element.
     *
     * @param element The element to search for in the version list
     * @return Whether the element was found in the version list
     */
    bool find(ChainedVersionRecord* element);

    /**
     * @brief Advances the iterator until it points to the element with the given version
     *
     * In case the element is not found in the list the iterator will point to the end of the list or some arbitrary
     * element.
     *
     * @param version The version of the element to search for in the version list
     * @return Whether the element was found in the version list
     */
    bool find(uint64_t version);

    /**
     * @brief Removes the current element from the version list
     *
     * Even in the case the operation succeeds the old element might still be contained in the list (though marked as
     * invalid) and as such it is not safe to release or reuse the memory associated with the element immediately after
     * calling this function. Only when the iterator points to the following element, the memory of the old element may
     * be released.
     *
     * Currently supports only removal from the head of the version list.
     *
     * @return Whether the element was successfully removed
     */
    bool remove();

    /**
     * @brief Replaces the current element from the version list with the given element
     *
     * Even in the case the operation succeeds the old element might still be contained in the list (though marked as
     * invalid) and as such it is not safe to release or reuse the memory associated with the element immediately after
     * calling this function. Only when the iterator points to the given replacement element, the memory of the old
     * element may be released.
     *
     * @param next Element to replace the current element with
     * @return Whether the element was successfully replaced
     */
    bool replace(ChainedVersionRecord* next);

    /**
     * @brief Inserts the given element into the version list before the current element
     *
     * @param element Element to insert into the version list
     * @return Whether the element was successfully inserted
     */
    bool insert(ChainedVersionRecord* element);

private:
    /**
     * @brief Retrieves the head element of the version list from the hash table
     */
    ChainedVersionRecord* retrieveHead(uint64_t key) {
        auto element = reinterpret_cast<ChainedVersionRecord*>(mTable.mHashMap.get(mTable.mTableId, key));
        LOG_ASSERT(!element || (element->key() == key), "Element in hash map is of different key");
        return element;
    }

    /**
     * @brief Expire the next element in the version list in the version of the current element
     *
     * The current element must have a valid next element.
     */
    void expireNextElement();

    /**
     * @brief Reactivate the next element in the version list
     *
     * The current element must have a valid next element.
     */
    void reactivateNextElement();

    /**
     * @brief Replaces the current invalid element with the given element
     *
     * The element must have been already invalidated. This operation only replaces the current element, the element's
     * mutable data has to be reloaded separately.
     *
     * @param next The next element in the version list to replace the current element with
     * @return Whether the replacement succeeded
     */
    bool replaceCurrentInvalidElement(ChainedVersionRecord* next) {
        return (mPrev ? removeCurrentInvalidFromPrevious(next) : removeCurrentInvalidFromHead(next));
    }

    /**
     * @brief Insert the element as the new head into the version list
     *
     * Iterator has to point to the head element. This operation only sets the current element, the element's mutable
     * data has to be reloaded separately.
     *
     * @param element The element to insert
     * @return Whether the insert was successful
     */
    bool insertHead(ChainedVersionRecord* element);

    /**
     * @brief Removes the current invalid head element from the version list and replaces it with next
     *
     * Iterator has to point to the head element and the element must have been already invalidated. This operation only
     * removes the current element, the element's mutable data has to be reloaded separately.
     *
     * @param next The next element in the version list to replace the current element with
     * @return Whether the removal was successful
     */
    bool removeCurrentInvalidFromHead(ChainedVersionRecord* next);

    /**
     * @brief Removes the current invalid non-head element from the version list and replaces it with next
     *
     * Iterator must not point to the head element and the element must have been already invalidated. This operation
     * only removes the current element, the element's mutable data has to be reloaded separately.
     *
     * @param next The next element in the version list to replace the current element with
     * @return Whether the removal was successful
     */
    bool removeCurrentInvalidFromPrevious(ChainedVersionRecord* next);

    /**
     * @brief Initializes the iterator with the current element or the next valid element.
     *
     * If the iterator encounters an invalid element it tries to fix the version list by removing the invalid element
     * from the list.
     */
    void setCurrentEntry();

    Table& mTable;
    const uint64_t mMinVersion;
    ChainedVersionRecord* mPrev;
    ChainedVersionRecord* mCurrent;
    MutableRecordData mPrevData;
    MutableRecordData mCurrentData;
};

bool VersionRecordIterator::find(ChainedVersionRecord* element) {
    for (; !done(); next()) {
        // Check if the iterator reached the element
        if (mCurrent == element) {
            return true;
        }

        // Check if the current element is already older than the given element
        if (mCurrent->validFrom() < element->validFrom()) {
            return false;
        }
    }

    return false;
}

bool VersionRecordIterator::find(uint64_t version) {
    for (; !done(); next()) {
        // Check if the iterator reached the element with the given version
        if (mCurrent->validFrom() == version) {
            return true;
        }

        // Check if the current element is already older than the given version
        if (mCurrent->validFrom() < version) {
            return false;
        }
    }

    return false;
}

bool VersionRecordIterator::remove() {
    LOG_ASSERT(!done(), "Remove only supported on valid element");
    LOG_ASSERT(isNewest(), "Remove only supported on the beginning");

    auto next = mCurrentData.next();
    auto res = mCurrent->tryInvalidate(mCurrentData, next);
    if (!res) {
        // The validTo version or the next pointer changed (the current element is still valid)
        if (!mCurrentData.isInvalid()) {
            return res;
        }

        // The current element is invalid - Reset the iterator to the next element
        next = mCurrentData.next();
    } else if (next) {
        reactivateNextElement();
    }
    removeCurrentInvalidFromHead(next);
    setCurrentEntry();
    return res;
}

bool VersionRecordIterator::replace(ChainedVersionRecord* next) {
    LOG_ASSERT(next->validFrom() == mCurrent->validFrom(), "Replace only supported on element with same version");
    LOG_ASSERT(!done(), "Replace only supported on valid element");

    next->mutableData(mCurrentData);
    auto res = mCurrent->tryInvalidate(mCurrentData, next);
    if (!res) {
        // The validTo version or the next pointer changed (the current element is still valid)
        if (!mCurrentData.isInvalid()) {
            return res;
        }

        // The current element is invalid
        next = mCurrentData.next();
    }
    replaceCurrentInvalidElement(next);
    setCurrentEntry();
    return res;
}

bool VersionRecordIterator::insert(ChainedVersionRecord* element) {
    LOG_ASSERT(isNewest(), "Insert only supported on the beginning");
    LOG_ASSERT(!mCurrent || mCurrent->validFrom() < element->validFrom(), "Version of the tuple to insert must be "
            "larger than current tuple");
    auto next = mCurrent;
    mCurrentData = MutableRecordData(ChainedVersionRecord::ACTIVE_VERSION, next, false);
    element->mutableData(mCurrentData);

    auto res = insertHead(element);
    if (!res) {
        setCurrentEntry();
    } else if (next) {
        expireNextElement();
    }

    return res;
}

void VersionRecordIterator::expireNextElement() {
    auto next = mCurrentData.next();
    LOG_ASSERT(next, "Next element is null while expiring element");

    auto nextData = next->mutableData();
    while (true) {
        LOG_ASSERT(nextData.validTo() == ChainedVersionRecord::ACTIVE_VERSION, "Next element is already set to expire");
        if (nextData.isInvalid()) {
            next = nextData.next();
            if (!next) {
                return;
            }
            nextData = next->mutableData();
            continue;
        }

        if (next->tryExpire(nextData, mCurrent->validFrom())) {
            return;
        }
    }
}

void VersionRecordIterator::reactivateNextElement() {
    auto next = mCurrentData.next();
    LOG_ASSERT(next, "Next element is null while reactivating element");

    auto nextData = next->mutableData();
    while (true) {
        LOG_ASSERT(nextData.validTo() == mCurrent->validFrom(), "Next element is not set to expire in the current "
                "element\'s version");
        if (nextData.isInvalid()) {
            next = nextData.next();
            if (!next) {
                return;
            }
            nextData = next->mutableData();
            continue;
        }

        if (next->tryReactivate(nextData)) {
            return;
        }
    }
}

bool VersionRecordIterator::insertHead(ChainedVersionRecord* element) {
    LOG_ASSERT(isNewest(), "Insert only supported on the beginning");
    void* actualData = nullptr;
    auto res = (mCurrent ? mTable.mHashMap.update(mTable.mTableId, element->key(), mCurrent, element, &actualData)
                         : mTable.mHashMap.insert(mTable.mTableId, element->key(), element, &actualData));
    mCurrent = (res ? element : reinterpret_cast<ChainedVersionRecord*>(actualData));
    return res;
}

bool VersionRecordIterator::removeCurrentInvalidFromHead(ChainedVersionRecord* next) {
    LOG_ASSERT(isNewest(), "Remove only supported on the beginning");
    LOG_ASSERT(!done(), "Remove not supported at the end");

    void* actualData = nullptr;
    auto res = (next ? mTable.mHashMap.update(mTable.mTableId, mCurrent->key(), mCurrent, next, &actualData)
                     : mTable.mHashMap.erase(mTable.mTableId, mCurrent->key(), mCurrent, &actualData));
    mCurrent = (res ? next : reinterpret_cast<ChainedVersionRecord*>(actualData));
    return res;
}

bool VersionRecordIterator::removeCurrentInvalidFromPrevious(ChainedVersionRecord* next) {
    LOG_ASSERT(!isNewest(), "Remove not supported on the beginning");
    LOG_ASSERT(!done(), "Remove not supported at the end");

    if (mPrev->tryNext(mPrevData, next)) {
        mCurrent = next;
        return true;
    }

    if (mPrevData.isInvalid()) {
        // The previous element itself got deleted from the version list - Retry from the beginning
        mCurrent = retrieveHead(mPrev->key());
        mPrev = nullptr;
        return false;
    }

    // The previous element is still valid but the next pointer changed
    // This happens when the current entry was recycled - Reset iterator to the updated pointer
    mCurrent = mPrevData.next();
    return false;
}

void VersionRecordIterator::setCurrentEntry() {
    while (mCurrent) {
        LOG_ASSERT(!mPrev || (mCurrent->key() == mPrev->key()), "Element in version list is of different key");

        mCurrentData = mCurrent->mutableData();
        if (!mCurrentData.isInvalid()) {
            return;
        }

        replaceCurrentInvalidElement(mCurrentData.next());
    }

    // Only reached when mRecord is null
    mCurrentData = MutableRecordData();
}

GcScanIterator::GcScanIterator(Table& table, const LogImpl::PageIterator& begin, const LogImpl::PageIterator& end,
        uint64_t minVersion, const Record* record)
        : mTable(table),
          mMinVersion(minVersion),
          mPagePrev(begin),
          mPageIt(begin),
          mPageEnd(end),
          mEntryIt(mPageIt == mPageEnd ? LogPage::EntryIterator() : mPageIt->begin()),
          mEntryEnd(mPageIt == mPageEnd ? LogPage::EntryIterator() : mPageIt->end()),
          mRecyclingHead(nullptr),
          mRecyclingTail(nullptr),
          mGarbage(0x0u),
          mSealed(false),
          mRecycle(false) {
    mCurrentEntry.mRecord = record;
    if (!done()) {
        setCurrentEntry();
    }
}

GcScanIterator::~GcScanIterator() {
    if (mRecyclingHead != nullptr) {
        LOG_ASSERT(mRecyclingTail, "Recycling tail is null despite head being non null");
        mTable.mLog.appendPage(mRecyclingHead, mRecyclingTail);
    }
}

GcScanIterator::GcScanIterator(GcScanIterator&& other)
        : mTable(other.mTable),
          mMinVersion(other.mMinVersion),
          mPagePrev(std::move(other.mPagePrev)),
          mPageIt(std::move(other.mPageIt)),
          mPageEnd(std::move(other.mPageEnd)),
          mEntryIt(std::move(other.mEntryIt)),
          mEntryEnd(std::move(other.mEntryEnd)),
          mRecyclingHead(other.mRecyclingHead),
          mRecyclingTail(other.mRecyclingTail),
          mGarbage(other.mGarbage),
          mSealed(other.mSealed),
          mRecycle(other.mRecycle),
          mCurrentEntry(std::move(other.mCurrentEntry)) {
    other.mRecyclingHead = nullptr;
    other.mRecyclingTail = nullptr;
    other.mGarbage = 0x0u;
    other.mSealed = false;
    other.mRecycle = false;
}

void GcScanIterator::next() {
    if (advanceEntry()) {
        setCurrentEntry();
    }
}

bool GcScanIterator::advanceEntry() {
    // Advance the iterator to the next entry
    if (++mEntryIt != mEntryEnd) {
        return true;
    }

    // Advance to next page
    if (mRecycle) {
        ++mPageIt;
        mTable.mLog.erase(mPagePrev.operator->(), mPageIt.operator->());
    } else {
        // Only store the garbage statistic when every entry in the page was sealed
        if (mSealed) {
            mPageIt->context().store(mGarbage);
        }
        mPagePrev = mPageIt++;
    }

    if (mPageIt == mPageEnd) {
        return false;
    }
    mEntryIt = mPageIt->begin();
    mEntryEnd = mPageIt->end();

    // Retrieve usage statistics of the current page
    mGarbage = 0x0u;
    uint32_t offset;
    std::tie(offset, mSealed) = mPageIt->offsetAndSealed();
    auto size = offset - mPageIt->context().load();
    mRecycle = (mSealed && ((size * 100) / LogPage::MAX_DATA_SIZE < gGcThreshold));

    return true;
}

void GcScanIterator::setCurrentEntry() {
    do {
        if (BOOST_UNLIKELY(!mEntryIt->sealed())) {
            LOG_ASSERT(!mRecycle, "Recycling page even though not all entries are sealed");
            mSealed = false;
            continue;
        }
        LOG_ASSERT(mEntryIt->size() >= sizeof(ChainedVersionRecord), "Log record is smaller than record header");

        auto record = reinterpret_cast<ChainedVersionRecord*>(mEntryIt->data());

        auto context = record->mutableData();
        if (context.isInvalid()) {
            // The element is already marked as invalid - Increase the garbage counter
            mGarbage += mEntryIt->entrySize();
            continue;
        }

        auto type = crossbow::from_underlying<VersionRecordType>(mEntryIt->type());
        if (context.validTo() < mMinVersion) {
            // No version can read the current element - Mark it as invalid and increase the garbage counter
#ifdef NDEBUG
            record->invalidate();
#else
            auto res = record->tryInvalidate(context, nullptr);
            LOG_ASSERT(res, "Invalidating expired element failed");
#endif
            mGarbage += mEntryIt->entrySize();
            continue;
        } else if ((type == VersionRecordType::DELETION) && (record->validFrom() < mMinVersion)) {
            // Try to mark the deletion as invalid and set the next pointer to null
            // This basically truncates the version list and marks the deletion entry as deleted in the version history
            // Because the entry is still alive (i.e. can be accessed by other transactions) we have to use a CAS to
            // invalidate the entry
            if (!record->tryInvalidate(context, nullptr)) {
                continue;
            }
            mGarbage += mEntryIt->entrySize();

            // Iterate over the whole version list for this key, this ensures the removal of the invalid deletion entry
            for (VersionRecordIterator recIter(mTable, mMinVersion, record->key()); !recIter.done(); recIter.next()) {
            }
            continue;
        }

        if (mRecycle) {
            recycleEntry(record, mEntryIt->size(), mEntryIt->type());
        }

        // Skip the element if it is not a data entry (i.e. deletion)
        if (type != VersionRecordType::DATA) {
            continue;
        }

        mCurrentEntry.mValidFrom = record->validFrom();
        mCurrentEntry.mValidTo = context.validTo();
        mCurrentEntry.mData = record->data();
        mCurrentEntry.mSize = mEntryIt->size() - sizeof(ChainedVersionRecord);
        return;

    } while (advanceEntry());
}

void GcScanIterator::recycleEntry(ChainedVersionRecord* oldElement, uint32_t size, uint32_t type) {
    if (mRecyclingHead == nullptr) {
        mRecyclingHead = new(mTable.mPageManager.alloc()) LogPage();
        if (mRecyclingHead == nullptr) {
            LOG_ERROR("PageManager ran out of space");
            mRecycle = false;
            return;
        }
        mRecyclingTail = mRecyclingHead;
    }

    auto newEntry = mRecyclingHead->append(size, type);
    if (newEntry == nullptr) {
        auto newHead = new(mTable.mPageManager.alloc()) LogPage();
        if (newHead == nullptr) {
            LOG_ERROR("PageManager ran out of space");
            mRecycle = false;
            return;
        }
        newHead->next().store(mRecyclingHead);
        mRecyclingHead->seal();
        mRecyclingHead = newHead;
        newEntry = mRecyclingHead->append(size, type);
        LOG_ASSERT(newEntry, "Unable to allocate entry on fresh page");
    }

    auto newElement = new (newEntry->data()) ChainedVersionRecord(oldElement->key(), oldElement->validFrom());
    memcpy(newElement->data(), oldElement->data(), size - sizeof(ChainedVersionRecord));

    if (!replaceElement(oldElement, newElement)) {
        newElement->invalidate();
    }

    newEntry->seal();
}

bool GcScanIterator::replaceElement(ChainedVersionRecord* oldElement, ChainedVersionRecord* newElement) {
    LOG_ASSERT(oldElement->key() == newElement->key(), "Keys do not match");

    // Search for the old element in the version list - if it was not found it has to be invalidated by somebody else
    VersionRecordIterator recIter(mTable, mMinVersion, oldElement->key());
    if (!recIter.find(oldElement)) {
        LOG_ASSERT(oldElement->mutableData().isInvalid(), "Old element not in version list but not invalid");
        return false;
    }

    // Replace can fail because the next pointer or validTo version of the current element has changed or it was
    // invalidated by someone else - if it was invalidated then the iterator will point to a different element
    while (!recIter.replace(newElement)) {
        if (recIter.value() != oldElement) {
            LOG_ASSERT(oldElement->mutableData().isInvalid(), "Old element not in version list but not invalid");
            return false;
        }
    }

    // A successful replace only guarantees that the old element was invalidated (and replaced with the new element) but
    // the old element could still be in the version list - Traverse the iterator until the new element is reached (this
    // ensures all previous elements were valid at some time and we can safely reuse the memory of the old element)
    if (!recIter.find(newElement)) {
        LOG_ASSERT(newElement->mutableData().isInvalid(), "New element not in version list but not invalid");
    }
    return true;
}

Table::Table(PageManager& pageManager, const Schema& schema, uint64_t tableId, VersionManager& versionManager,
        HashTable& hashMap)
        : mPageManager(pageManager),
          mVersionManager(versionManager),
          mHashMap(hashMap),
          mRecord(schema),
          mTableId(tableId),
          mLog(mPageManager) {
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

std::vector<Table::Iterator> Table::startScan(int numThreads) {
    std::vector<Table::Iterator> result;
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
        for (auto j = 0; j < step && iter != end; ++j, ++iter) {
        }

        result.emplace_back(*this, begin, iter, version, &mRecord);
        begin = iter;
    }

    // The last scan takes the remaining pages
    result.emplace_back(*this, begin, end, version, &mRecord);

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

StoreImpl<Implementation::LOGSTRUCTURED_MEMORY>::StoreImpl(const StorageConfig& config)
        : mPageManager(PageManager::construct(config.totalMemory)),
          mTableManager(*mPageManager, config, mGc, mVersionManager),
          mHashMap(config.hashMapCapacity) {
}

} // namespace store
} // namespace tell
