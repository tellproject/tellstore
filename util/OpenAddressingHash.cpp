#include "OpenAddressingHash.hpp"

#include "helper.hpp"
#include "Logging.hpp"

#include <boost/functional/hash.hpp>

namespace tell {
namespace store {

namespace {

/**
 * @brief Check the wellformedness of tableId and key
 */
void checkDataKey(uint64_t table, uint64_t /* key */) {
    LOG_ASSERT(table != 0x0u, "Table ID must not be 0");
}

/**
 * @brief Check the wellformedness of data pointers
 */
void checkDataPtr(const void* ptr) {
    LOG_ASSERT(ptr != nullptr, "Data pointer not allowed to be null");
    LOG_ASSERT((reinterpret_cast<uintptr_t>(ptr) % 8 == 0), "Pointer not 8 byte aligned");
}

} // anonymous namespace

OpenAddressingTable::OpenAddressingTable(size_t capacity)
        : mCapacity(capacity),
          mBuckets(new Entry[mCapacity]),
          mTableHash(mCapacity),
          mKeyHash(mCapacity) {
}

OpenAddressingTable::~OpenAddressingTable() {
    delete[] mBuckets;
}

const void* OpenAddressingTable::get(uint64_t table, uint64_t key) const {
    checkDataKey(table, key);

    return execOnElement(table, key, static_cast<const void*>(nullptr), [] (const Entry& /* entry */, const void* ptr) {
        return ptr;
    });
}

bool OpenAddressingTable::insert(uint64_t table, uint64_t key, void* data, void** actualData /* = nullptr */) {
    checkDataKey(table, key);
    checkDataPtr(data);

    auto hash = calculateHash(table, key);
    for (auto pos = hash;; ++pos) {
        auto& entry = mBuckets[pos % mCapacity];
        auto ptr = entry.ptr.load();

        // Check if pointer is already acquired (i.e. not a deletion and not free) - Skip bucket if it is
        // There is no need to check if the insertion will conflict as this will be resolved at a later time.
        if ((ptr != (to_underlying(EntryMarker::DELETED)) && (ptr != to_underlying(EntryMarker::FREE)))) {
            continue;
        }

        // Try to claim this bucket by setting the pointer to inserting
        // If this fails somebody else claimed the bucket in the mean time
        auto insertPtr = (reinterpret_cast<uintptr_t>(data) | to_underlying(EntryMarker::INSERTING));
        if (!entry.ptr.compare_exchange_strong(ptr, insertPtr)) {
            continue;
        }

        // Write the key and table information
        // Table has to be written last as it is read last by readers to detect if key was written correctly
        entry.keyId.store(key);
        entry.tableId.store(table);

        // Check if another insert conflicts with ours
        // If this fails somebody else is inserting or has already inserted the same key, set pointer to invalid and
        // free bucket.
        if (hasInsertConflict(hash, pos, table, key, actualData)) {
            entry.ptr.store(to_underlying(EntryMarker::INVALID));
            deleteEntry(entry);
            return false;
        }

        // Try to set the pointer from null or deleted to our data pointer
        // If this fails somebody else set the element to invalid and the bucket has to be released
        if (!entry.ptr.compare_exchange_strong(insertPtr, reinterpret_cast<uintptr_t>(data))) {
            deleteEntry(entry);

            if (actualData) *actualData = nullptr;
            return false;
        }

        return true;
    }
}

bool OpenAddressingTable::update(uint64_t table, uint64_t key, const void* oldData, void* newData,
        void** actualData /* = nullptr */) {
    checkDataKey(table, key);
    checkDataPtr(oldData);
    checkDataPtr(newData);

    return execOnElement(table, key, false, [table, key, oldData, newData, actualData] (Entry& entry, void* /* ptr */) {
        auto expected = reinterpret_cast<uintptr_t>(oldData);
        if (entry.ptr.compare_exchange_strong(expected, reinterpret_cast<uintptr_t>(newData))) {
            return true;
        }

        setActualData(table, key, actualData, entry, expected);
        return false;
    });
}

bool OpenAddressingTable::erase(uint64_t table, uint64_t key, const void* oldData, void** actualData /* = nullptr */) {
    checkDataKey(table, key);
    checkDataPtr(oldData);

    return execOnElement(table, key, true, [table, key, oldData, actualData] (Entry& entry, const void* /* ptr */) {
        auto expected = reinterpret_cast<uintptr_t>(oldData);
        if (entry.ptr.compare_exchange_strong(expected, to_underlying(EntryMarker::INVALID))) {
            deleteEntry(entry);
            return true;
        }

        setActualData(table, key, actualData, entry, expected);
        return false;
    });
}

void OpenAddressingTable::deleteEntry(Entry& entry) {
    entry.tableId.store(0x0u);
    entry.keyId.store(0x0u);
    entry.ptr.store(to_underlying(EntryMarker::DELETED));
}

void OpenAddressingTable::setActualData(uint64_t table, uint64_t key, void** actualData, const Entry& entry,
        uintptr_t ptr) {
    if (!actualData) {
        return;
    }

    while (true) {
        // Check if pointer is marked as deleted, inserting or invalid (it will never be free)
        if ((ptr & MARKER_MASK) != 0x0u) {
            *actualData = nullptr;
            return;
        }

        auto t = entry.tableId.load();
        auto k = entry.keyId.load();

        // Check if the pointer changed - Reload variables if it has
        // Needed to ensure the loaded ptr, table and key are consistent and were not changed by other threads in the
        // meantime. This does not suffer from the ABA problem as the table requires unique pointers and access through
        // an epoch.
        auto p = entry.ptr.load();
        if (p != ptr) {
            ptr = p;
            continue;
        }

        // Check if the table and key of the bucket changed
        if (t != table || k != key) {
            *actualData = nullptr;
            return;
        }

        *actualData = reinterpret_cast<void*>(ptr);
        return;
    }
}

uint64_t OpenAddressingTable::calculateHash(uint64_t table, uint64_t key) const {
    auto hash = mTableHash(table);
    boost::hash_combine(hash, mKeyHash(key));
    return hash;
}

bool OpenAddressingTable::hasInsertConflict(size_t hash, size_t insertPos, uint64_t table, uint64_t key,
        void** actualData) {
    auto pos = hash;
    while (true) {
        // Skip our own entry
        if (pos == insertPos) {
            ++pos;
            continue;
        }

        auto& entry = mBuckets[pos % mCapacity];
        auto ptr = entry.ptr.load();

        // Check if we reached the end of the overflow bucket
        if (ptr == to_underlying(EntryMarker::FREE)) {
            LOG_ASSERT(pos > insertPos, "Reached end of overflow before reaching insert element");
            return false;
        }

        // Check if pointer is invalid or deleted - Skip bucket if it is
        if ((ptr & (to_underlying(EntryMarker::DELETED) | to_underlying(EntryMarker::INVALID))) != 0x0u) {
            ++pos;
            continue;
        }

        // At this point element is either inserting or valid
        auto t = entry.tableId.load();
        auto k = entry.keyId.load();

        // Check if another thread is still writing data to the bucket - Skip bucket if it is
        // The other insert thread has not yet entered the conflict detection scan, if the insert conflicts with this
        // insert then the other thread will detect it as the current thread already wrote the table, key and pointer
        // fields. A table ID of 0 means either deleted or beginning of insert or beginning of deletion as such this
        // check can be made before we check the consistency in the next step.
        if (t == 0x0u) {
            ++pos;
            continue;
        }

        // Check if the pointer changed - Reload variables if it has
        // Needed to ensure the loaded ptr, table and key are consistent and were not changed by other threads in the
        // meantime. This does not suffer from the ABA problem as the table requires unique pointers and access through
        // an epoch.
        if (entry.ptr.load() != ptr) {
            continue;
        }

        // Check if bucket belongs to another element, skip bucket if it does
        if (t != table || k != key) {
            ++pos;
            continue;
        }

        // Check whether the pointer is already set (i.e. has no inserting marker) or marks a concurrent insert and the
        // element of the other thread is located in a bucket before our own insert bucket
        if ((ptr & to_underlying(EntryMarker::INSERTING)) == 0x0u) {
            // No concurrent insert, no deletion, no invalid, no free element as such the pointer has to be valid
            // Abort as another thread already completed the insert
            if (actualData) *actualData = reinterpret_cast<void*>(ptr);
            return true;
        } else if (pos < insertPos) {
            // Concurrent insert and the element of the other thread is located in a bucket before our own insert bucket
            // Abort our own insert
            if (actualData) *actualData = nullptr;
            return true;
        }

        // The element is located in a bucket after our own insert bucket so we try to set the other pointer to invalid
        // If this fails then the other thread might have completed its insert operation and we have to abort. This does
        // not suffer from the ABA problem: The pointer points to the element with the INSERTING marker set, as the
        // table accesses elements only through the epoch the element's memory will only be reused after this thread
        // exits.
        auto expected = ptr;
        if (entry.ptr.compare_exchange_strong(expected, to_underlying(EntryMarker::INVALID))) {
            ++pos;
            continue;
        }

        // Check if pointer is invalid or deleted, skip bucket if it is
        if ((expected & (to_underlying(EntryMarker::DELETED) | to_underlying(EntryMarker::INVALID))) != 0x0u) {
            ++pos;
            continue;
        }

        // Check if the pointer is already set (not null without marker)
        if ((ptr & POINTER_MASK) == expected) {
            if (actualData) *actualData = reinterpret_cast<void*>(expected);
            return true;
        }

        // The pointer changed completely - Recheck the current bucket
    }
}

template <typename T, typename F>
T OpenAddressingTable::execOnElement(uint64_t table, uint64_t key, T notFound, F fun) const {
    auto pos = calculateHash(table, key);
    while (true) {
        auto& entry = mBuckets[pos % mCapacity];
        auto ptr = entry.ptr.load();

        // Check if pointer is marked as deleted, inserting or invalid - Skip bucket if it is
        if ((ptr & MARKER_MASK) != to_underlying(EntryMarker::FREE)) {
            ++pos;
            continue;
        }

        // Check if this bucket marks the end of the overflow bucket - Return not-found if it is
        if (ptr == to_underlying(EntryMarker::FREE)) {
            return notFound;
        }

        auto t = entry.tableId.load();
        auto k = entry.keyId.load();

        // Check if the pointer changed - Reload variables if it has
        // Needed to ensure the loaded ptr, table and key are consistent and were not changed by other threads in the
        // meantime. This does not suffer from the ABA problem as the table requires unique pointers and access through
        // an epoch.
        if (entry.ptr.load() != ptr) {
            continue;
        }

        // Check if bucket belongs to another element, skip bucket if it does
        if (t != table || k != key) {
            ++pos;
            continue;
        }

        // Element found
        return fun(entry, reinterpret_cast<const void*>(ptr));
    }
}

template <typename T, typename F>
T OpenAddressingTable::execOnElement(uint64_t table, uint64_t key, T notFound, F fun) {
    return const_cast<const OpenAddressingTable*>(this)->execOnElement(table, key, notFound,
            [&fun] (const Entry& entry, const void* ptr) {
        return fun(const_cast<Entry&>(entry), const_cast<void*>(ptr));
    });
}

} // namespace store
} // namespace tell
