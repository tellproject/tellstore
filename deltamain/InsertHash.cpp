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

#include "InsertHash.hpp"

#include <crossbow/logger.hpp>

namespace tell {
namespace store {
namespace deltamain {

InsertTable::InsertTable(size_t capacity)
        : mCapacity(capacity),
          mBuckets(new AtomicEntry[mCapacity]),
          mHash(mCapacity) {
}

const void* InsertTable::get(uint64_t key) const {
    return execOnElement(key, static_cast<const void*>(nullptr), [] (const AtomicEntry& /* entry */, uintptr_t ptr) {
        return reinterpret_cast<const void*>(ptr);
    });
}

bool InsertTable::insert(uint64_t key, void* data, void** actualData /* = nullptr */) {
    LOG_ASSERT(data != nullptr, "Data pointer not allowed to be null");

    auto hash = mHash(key);
    for (auto pos = hash; pos < (hash + mCapacity); ++pos) {
        auto& entry = mBuckets[pos % mCapacity];

        // If the pointer is null then we reached the end of the overflow bucket and the element was not found
        auto ptr = entry.loadValue();
        if (ptr == crossbow::to_underlying(EntryMarker::FREE)) {
            // Try to claim the current bucket
            Entry oldEntry;
            Entry newEntry(key, reinterpret_cast<uintptr_t>(data));
            if (entry.compare_exchange_strong(oldEntry, newEntry)) {
                return true;
            }

            // Move to the next bucket if the current one was claimed by an insert with a different key
            if (oldEntry.key != key) {
                continue;
            }

            // Update the pointer
            ptr = oldEntry.value;
        } else {
            // If the stored key is different than the target key we have to search the next bucket
            auto k = entry.loadKey();
            if (k != key) {
                continue;
            }
        }

        // Try to claim the bucket if the pointer marks a deletion
        if (ptr == crossbow::to_underlying(EntryMarker::DELETED)
                && entry.updateValue(ptr, reinterpret_cast<uintptr_t>(data))) {
            return true;
        }

        // The element was not deleted
        if (actualData) *actualData = reinterpret_cast<void*>(ptr);
        return false;
    }

    LOG_ERROR("Hash table is full");
    return false;
}

bool InsertTable::update(uint64_t key, const void* oldData, void* newData, void** actualData /* = nullptr */) {
    LOG_ASSERT(newData != nullptr, "Data pointer not allowed to be null");
    return internalUpdate(key, oldData, reinterpret_cast<uintptr_t>(newData), actualData);
}

bool InsertTable::remove(uint64_t key, const void* oldData, void** actualData /* = nullptr */) {
    return internalUpdate(key, oldData, crossbow::to_underlying(EntryMarker::DELETED), actualData);
}

bool InsertTable::internalUpdate(uint64_t key, const void* oldData, uintptr_t newData, void** actualData) {
    return execOnElement(key, false, [oldData, newData, actualData] (AtomicEntry& entry, uintptr_t ptr) {
        auto expected = reinterpret_cast<uintptr_t>(oldData);
        if (ptr != expected) {
            if (actualData) *actualData = reinterpret_cast<void*>(ptr);
            return false;
        }

        // Try to update the pointer
        if (entry.updateValue(expected, newData)) {
            return true;
        }

        // Set the actual data only in case its not a deletion
        if (expected != crossbow::to_underlying(EntryMarker::DELETED)) {
            if (actualData) *actualData = reinterpret_cast<void*>(expected);
        }
        return false;
    });
}

template <typename T, typename F>
T InsertTable::execOnElement(uint64_t key, T notFound, F fun) const {
    auto hash = mHash(key);
    for (auto pos = hash; pos < (hash + mCapacity); ++pos) {
        auto& entry = mBuckets[pos % mCapacity];

        // If the pointer is null then we reached the end of the overflow bucket and the element was not found
        auto ptr = entry.loadValue();
        if (ptr == crossbow::to_underlying(EntryMarker::FREE)) {
            return notFound;
        }

        // If the stored key is different than the target key we have to search the next bucket
        auto k = entry.loadKey();
        if (k != key) {
            continue;
        }

        // The entry was marked as deleted
        if (ptr == crossbow::to_underlying(EntryMarker::DELETED)) {
            return notFound;
        }

        // The element was found
        return fun(entry, ptr);
    }

    LOG_ERROR("Hash table is full");
    return notFound;
}

template <typename T, typename F>
T InsertTable::execOnElement(uint64_t key, T notFound, F fun) {
    return const_cast<const InsertTable*>(this)->execOnElement(key, notFound,
            [&fun] (const AtomicEntry& entry, uintptr_t ptr) {
        return fun(const_cast<AtomicEntry&>(entry), ptr);
    });
}

} // namespace deltamain
} // namespace store
} // namespace tell
