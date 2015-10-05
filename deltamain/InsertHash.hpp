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

#pragma once

#include <util/functional.hpp>

#include <atomic>
#include <cstdint>
#include <memory>

namespace tell {
namespace store {
namespace deltamain {

/**
 * @brief Lock-Free Open-Addressing hash table for associating a pointer with a key
 *
 * The hash table is designed to only support inserts and gets of the data.
 */
class InsertTable {
public:
    InsertTable(size_t capacity);

    size_t capacity() const {
        return mCapacity;
    }

    /**
     * @brief Looks up the element in the hash table
     *
     * @param key The key ID of the entry
     * @return The associated data pointer or nullptr if the element did not exist
     */
    void* get(uint64_t key) {
        return const_cast<void*>(const_cast<const InsertTable*>(this)->get(key));
    }

    /**
     * @brief Looks up the element in the hash table
     *
     * @param key The key ID of the entry
     * @return The associated data pointer or nullptr if the element did not exist
     */
    const void* get(uint64_t key) const;

    /**
     * @brief Tries to insert the element into the hash table
     *
     * @param key The key ID of the entry
     * @param data The new data pointer
     * @param actualData Pointer to the element which caused the conflict
     * @return True if the insert was successful, false if the element already exists in the hash table
     */
    bool insert(uint64_t key, void* data, void** actualData = nullptr);

private:
    /**
     * @brief Struct holding the bucket data
     */
    struct alignas(16) Entry {
        Entry()
                : key(0x0u),
                  value(nullptr) {
        }

        Entry(uint64_t k, void* v)
                : key(k),
                  value(v) {
        }

        uint64_t key;
        void* value;
    };

    /**
     * @brief Helper class providing atomic access to the key, value or the whole entry
     */
    class AtomicEntry {
    public:
        static_assert(sizeof(Entry) == 16, "Only 16 byte types are supported");
        static_assert(alignof(Entry) == 16, "Only 16 byte aligned types are supported");

        uint64_t loadKey() const noexcept {
            uint64_t key;
            __atomic_load(&mEntry.key, &key, std::memory_order_seq_cst);
            return key;
        }

        void* loadValue() const noexcept {
            void* value;
            __atomic_load(&mEntry.value, &value, std::memory_order_seq_cst);
            return value;
        }

        bool compare_exchange_strong(Entry& expected, Entry desired) noexcept {
            return __atomic_compare_exchange(&mEntry, &expected, &desired, false, std::memory_order_seq_cst,
                    std::memory_order_seq_cst);
        }

    private:
        Entry mEntry;
    };

    size_t mCapacity;

    std::unique_ptr<AtomicEntry[]> mBuckets;

    cuckoo_hash_function mHash;
};

} // namespace deltamain
} // namespace store
} // namespace tell
