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
    auto hash = mHash(key);
    for (auto pos = hash; pos < (hash + mCapacity); ++pos) {
        auto& entry = mBuckets[pos % mCapacity];

        // If the pointer is null then we reached the end of the overflow bucket and the element was not found
        auto ptr = entry.loadValue();
        if (ptr == nullptr) {
            return nullptr;
        }

        // If the stored key is different than the target key we have to search the next bucket
        auto k = entry.loadKey();
        if (k != key) {
            continue;
        }

        // The element was found
        return ptr;
    }

    LOG_ERROR("Hash table is full");
    return nullptr;
}

bool InsertTable::insert(uint64_t key, void* data, void** actualData /* = nullptr */) {
    LOG_ASSERT(data != nullptr, "Data pointer not allowed to be null");

    Entry newEntry(key, data);
    auto hash = mHash(key);
    for (auto pos = hash; pos < (hash + mCapacity); ++pos) {
        auto& entry = mBuckets[pos % mCapacity];

        // If the pointer is null then we reached the end of the overflow bucket and the element was not found
        auto ptr = entry.loadValue();
        if (ptr == nullptr) {
            Entry oldEntry;
            if (entry.compare_exchange_strong(oldEntry, newEntry)) {
                return true;
            }

            if (oldEntry.key == key) {
                if (actualData) *actualData = oldEntry.value;
                return false;
            }

            continue;
        }

        // If the stored key is different than the target key we have to search the next bucket
        auto k = entry.loadKey();
        if (k != key) {
            continue;
        }

        // The element was already inserted
        if (actualData) *actualData = ptr;
        return false;
    }

    LOG_ERROR("Hash table is full");
    return false;
}

} // namespace deltamain
} // namespace store
} // namespace tell
