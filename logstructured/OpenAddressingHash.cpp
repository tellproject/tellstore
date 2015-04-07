#include "OpenAddressingHash.hpp"

#include <util/Logging.hpp>

#include <boost/functional/hash.hpp>

namespace tell {
namespace store {
namespace logstructured {

namespace {

static constexpr void* gDeletedPtr = reinterpret_cast<void*>(0x1u);
static constexpr void* gInvalidPtr = reinterpret_cast<void*>(0x2u);

/**
 * @brief Check the wellformedness of tableId and keyId
 */
void checkDataKey(uint64_t table, uint64_t key) {
    LOG_ASSERT(table != 0x0u, "Table ID must not be 0");
    LOG_ASSERT(key != 0x0u, "Key ID must not be 0");
}

/**
 * @brief Check the wellformedness of data pointers
 */
void checkDataPtr(void* ptr) {
    LOG_ASSERT(ptr != nullptr, "Data pointer not allowed to be null");
    LOG_ASSERT(ptr != gDeletedPtr, "The two LSBs have to be zero");
    LOG_ASSERT(ptr != gInvalidPtr, "The two LSBs have to be zero");
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

void* OpenAddressingTable::get(uint64_t table, uint64_t key) {
    checkDataKey(table, key);

    return execOnElement(table, key, static_cast<void*>(nullptr), [] (Entry& /* entry */, void* ptr) {
        return ptr;
    });
}

bool OpenAddressingTable::insert(uint64_t table, uint64_t key, void* data) {
    checkDataKey(table, key);
    checkDataPtr(data);

    auto hash = calculateHash(table, key);
    for (auto pos = hash;; ++pos) {
        auto& entry = mBuckets[pos % mCapacity];
        auto ptr = entry.ptr.load();

        // Check if pointer is invalid
        if (ptr == gInvalidPtr) {
            continue;
        }

        // Check if pointer denotes a valid element
        if (ptr != nullptr && ptr != gDeletedPtr) {
            // Check if bucket belongs to another element, skip bucket if it does
            if (entry.tableId.load() != table || entry.keyId.load() != key) {
                continue;
            }

            // Entry exists
            return false;
        }

        // Try to claim this bucket by setting the table
        // If this fails somebody else claimed the bucket in the mean time
        uint64_t t = 0x0u;
        if (!entry.tableId.compare_exchange_strong(t, table)) {
            continue;
        }
        entry.keyId.store(key);

        // Check if another concurrent insert conflicts with ours
        // If this fails somebody else is inserting the same key, set pointer to invalid and free bucket
        if (hasInsertConflict(hash, pos, table, key)) {
            entry.ptr.store(gInvalidPtr);
            deleteEntry(entry);
            return false;
        }

        // Try to set the pointer from null or deleted to our data pointer
        // If this fails somebody else set us to invalid, so we have to free bucket
        if (!entry.ptr.compare_exchange_strong(ptr, data)) {
            deleteEntry(entry);
            return false;
        }

        return true;
    }
}

bool OpenAddressingTable::update(uint64_t table, uint64_t key, void* oldData, void* newData) {
    checkDataKey(table, key);
    checkDataPtr(oldData);
    checkDataPtr(newData);

    return execOnElement(table, key, false, [oldData, newData] (Entry& entry, void* /* ptr */) {
        // Set the element pointer from old to new
        auto expected = oldData;
        return entry.ptr.compare_exchange_strong(expected, newData);
    });
}

bool OpenAddressingTable::erase(uint64_t table, uint64_t key, void* oldData) {
    checkDataKey(table, key);
    checkDataPtr(oldData);

    return execOnElement(table, key, true, [oldData] (Entry& entry, void* /* ptr */) {
        // Try to set the pointer to invalid
        // If this fails then somebody else modified the element
        auto expected = oldData;
        if (!entry.ptr.compare_exchange_strong(expected, gInvalidPtr)) {
            return false;
        }
        deleteEntry(entry);

        return true;
    });
}

void OpenAddressingTable::deleteEntry(Entry& entry) {
    entry.tableId.store(0x0u);
    entry.keyId.store(0x0u);
    entry.ptr.store(gDeletedPtr);
}

uint64_t OpenAddressingTable::calculateHash(uint64_t table, uint64_t key) {
    auto hash = mTableHash(table);
    boost::hash_combine(hash, mKeyHash(key));
    return hash;
}

bool OpenAddressingTable::hasInsertConflict(size_t hash, size_t insertPos, uint64_t table, uint64_t key) {
    for (auto pos = hash;; ++pos) {
        // Skip our own entry
        if (pos == insertPos) {
            continue;
        }

        auto& entry = mBuckets[pos % mCapacity];
        auto ptr = entry.ptr.load();

        // Check if pointer is invalid, skip bucket if it is
        if (ptr == gInvalidPtr) {
            continue;
        }

        auto t = entry.tableId.load();

        // Check if we reached the end of the overflow bucket and nobody is writing to this bucket
        if (ptr == nullptr && t == 0x0u) {
            LOG_ASSERT(pos > insertPos, "Reached end of overflow before reaching insert element");
            return false;
        }

        // Check if bucket belongs to another element, skip bucket if it does
        if (t != table || entry.keyId.load() != key) {
            continue;
        }

        // Check if the pointer is already set (not null without marker)
        // In this case we have to abort the insert, because another thread already completed the insert
        if ((reinterpret_cast<uintptr_t>(ptr) >> 2) != 0x0u) {
            return true;
        }

        // At this point there is a concurrent insert conflict but both threads did not yet complete the insert
        // We conflict if the element of the other thread is located in a bucket before our own insert bucket
        if (pos < insertPos) {
            return true;
        }

        // The element is located in a bucket after our own insert bucket so we try to set the other pointer to invalid
        // If this fails then the other thread completed its insert operation in the meantime and we have to abort
        if (!entry.ptr.compare_exchange_strong(ptr, gInvalidPtr)) {
            if (ptr != gInvalidPtr) {
                return true;
            }
        }
    }
}

template <typename T, typename F>
T OpenAddressingTable::execOnElement(uint64_t table, uint64_t key, T notFound, F fun) {
    for (auto pos = calculateHash(table, key);; ++pos) {
        auto& entry = mBuckets[pos % mCapacity];
        auto ptr = entry.ptr.load();

        // Check if pointer is invalid or deleted, skip bucket if it is
        if ((reinterpret_cast<uintptr_t>(ptr) & (0x2u | 0x1u)) != 0) {
            continue;
        }

        auto t = entry.tableId.load();

        // Check if we reached the end of the overflow bucket
        if (ptr == nullptr) {
            // Check if somebody else is currently writing to this bucket, skip bucket if this is the case
            if (t != 0x0u) {
                continue;
            }

            // End of overflow bucket, element not found
            return notFound;
        }

        // Check if bucket belongs to another element, skip bucket if it does
        if (t != table || entry.keyId.load() != key) {
            continue;
        }

        // Element found
        return fun(entry, ptr);
    }
}

} // namespace logstructured
} // namespace store
} // namespace tell
