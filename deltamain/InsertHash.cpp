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

#include <util/Log.hpp>

#include <crossbow/allocator.hpp>
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

InsertLogTable::InsertLogTable(size_t capacity)
    : mHeadList(crossbow::allocator::construct<InsertLogTableEntry>(nullptr, capacity)) {
}

InsertLogTable::~InsertLogTable() {
    auto headList = mHeadList.exchange(nullptr);
    while (headList != nullptr) {
        auto previousList = headList->nextList.load();
        crossbow::allocator::destroy_now(headList);
        headList = previousList;
    }
}

const InsertLogEntry* InsertLogTable::get(uint64_t key, InsertLogTableEntry** headList) const {
    auto insertList = mHeadList.load();
    if (headList) *headList = insertList;
    LOG_ASSERT(insertList != nullptr, "Head insert list must never be null");

    for (auto currentList = insertList; currentList; currentList = currentList->nextList.load()) {
        auto ptr = currentList->table.get(key);
        if (!ptr) {
            continue;
        }
        auto entry = LogEntry::entryFromData(reinterpret_cast<const char*>(ptr));

        // Check if entry is unsealed or invalid: In this case it can not be in an older insert table
        if (BOOST_UNLIKELY(!entry->sealed())) {
            break;
        }

        return reinterpret_cast<const InsertLogEntry*>(ptr);
    }

    return nullptr;
}

bool InsertLogTable::insert(uint64_t key, InsertLogEntry* record, InsertLogTableEntry* headList) {
    while (true) {
        // Try to insert the element in the insert table
        if (!headList->table.insert(key, record)) {
            return false;
        }

        // Check if a new head table was allocated in the meantime
        // The insert has to be repeated in the new head table
        auto newHeadList = mHeadList.load();
        if (newHeadList == headList) {
            return true;
        }

        // Check if an insert took place that in the older tables (up to the previous insert table)
        for (auto currentList = newHeadList->nextList.load(); currentList && currentList != headList;
                currentList = currentList->nextList.load()) {
            if (currentList->table.get(key)) {
                // The element was found in a newer insert table: Abort the current insertion
                return false;
            }
        }

        // Restart the insert in the newest table
        headList = newHeadList;
    }
}

bool InsertLogTable::remove(uint64_t key, const InsertLogEntry* oldRecord, InsertLogTableEntry* tailList) {
    auto insertList = mHeadList.load();
    auto endList = tailList->nextList.load();
    LOG_ASSERT(insertList != nullptr, "Head insert list must never be null");

    for (auto currentList = insertList; currentList || currentList == endList;
            currentList = currentList->nextList.load()) {
        // Try to remove the element in the insert table
        void* actualData = nullptr;
        if (currentList->table.remove(key, oldRecord, &actualData)) {
            return true;
        }

        // Check if the element does not exist in the current table, retry with the next
        if (actualData == nullptr) {
            continue;
        }

        // Element exists and changed in the meantime
        return false;
    }

    // Element was not found
    return false;
}

InsertLogTableEntry* InsertLogTable::allocateHead() {
    auto headList = mHeadList.load();
    auto newHeadList = crossbow::allocator::construct<InsertLogTableEntry>(headList, headList->table.capacity());

    // Set the newly allocated table as new head table
    // If this fails another hash table was allocated in the meantime by somebody else
    if (!mHeadList.compare_exchange_strong(headList, newHeadList)) {
        crossbow::allocator::destroy_now(newHeadList);
        newHeadList = headList;
    }
    return newHeadList;
}

void InsertLogTable::truncate(InsertLogTableEntry* endList) {
    // Truncate the insert hash table and free all tables using the epoch mechanism
    auto oldList = endList->nextList.exchange(nullptr);
    while (oldList != nullptr) {
        auto previousList = oldList->nextList.load();
        crossbow::allocator::destroy(oldList);
        oldList = previousList;
    }
}

} // namespace deltamain
} // namespace store
} // namespace tell
