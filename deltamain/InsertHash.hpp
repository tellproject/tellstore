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

#include <crossbow/enum_underlying.hpp>

#include <atomic>
#include <cstdint>
#include <memory>

namespace tell {
namespace store {
namespace deltamain {

class InsertLogEntry;

/**
 * @brief Lock-Free Open-Addressing hash table for associating a pointer with a key
 *
 * Space occupied by deleted keys is never reclaimed.
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

    /**
     * @brief Tries to update the already existing element in the hash table
     *
     * Atomically changes the value pointer from the old to the new pointer.
     *
     * @param key The key ID of the entry
     * @param oldData The old data pointer
     * @param newData The new data pointer
     * @param actualData Pointer to the element which caused the conflict
     * @return True if the update was successful, false if the element does not exist or the data was changed
     */
    bool update(uint64_t key, const void* oldData, void* newData, void** actualData = nullptr);

    /**
     * @brief Tries to remove the already existing element in the hash table
     *
     * @param key The key ID of the entry
     * @param oldData The old data pointer
     * @param actualData Pointer to the element which caused the conflict
     * @return True if the removal was successful, false if the element does not exist or the data was changed
     */
    bool remove(uint64_t key, const void* oldData, void** actualData = nullptr);

private:
    /**
     * @brief The potential states a Entry pointer can be tagged with
     */
    enum class EntryMarker : uintptr_t {
        /// The entry is still free
        FREE = 0x0u,

        /// The entry is deleted and can be reused (actual pointer will be null)
        DELETED = 0x1u,
    };

    /**
     * @brief Struct holding the bucket data
     */
    struct alignas(16) Entry {
        Entry()
                : key(0x0u),
                  value(crossbow::to_underlying(EntryMarker::FREE)) {
        }

        Entry(uint64_t k, uintptr_t v)
                : key(k),
                  value(v) {
        }

        uint64_t key;
        uintptr_t value;
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

        uintptr_t loadValue() const noexcept {
            uintptr_t value;
            __atomic_load(&mEntry.value, &value, std::memory_order_seq_cst);
            return value;
        }

        bool updateValue(uintptr_t& expected, uintptr_t desired) noexcept {
            return __atomic_compare_exchange(&mEntry.value, &expected, &desired, false, std::memory_order_seq_cst,
                    std::memory_order_seq_cst);
        }

        bool compare_exchange_strong(Entry& expected, Entry desired) noexcept {
            return __atomic_compare_exchange(&mEntry, &expected, &desired, false, std::memory_order_seq_cst,
                    std::memory_order_seq_cst);
        }

    private:
        Entry mEntry;
    };

    /**
     * @brief Searches the hash table for the element with key and executes the function on the element
     *
     * @param key The key ID of the entry
     * @param notFound The value to return when the element was not found
     * @param fun The function to be executed on the target element, interface must match T fun(AtomicEntry&, void* ptr)
     * @return The return value of fun or notFound if the element was not found
     */
    template <typename T, typename F>
    T execOnElement(uint64_t key, T notFound, F fun) const;

    template <typename T, typename F>
    T execOnElement(uint64_t key, T notFound, F fun);

    /**
     * @brief Tries to update the existing element
     *
     * Used internally by delete and update to change the stored pointer.
     */
    bool internalUpdate(uint64_t key, const void* oldData, uintptr_t newData, void** actualData);

    size_t mCapacity;

    std::unique_ptr<AtomicEntry[]> mBuckets;

    cuckoo_hash_function mHash;
};

struct InsertLogTableEntry {
    InsertLogTableEntry(InsertLogTableEntry* next, uint64_t capacity)
            : nextList(next),
              table(capacity) {
    }

    /// Pointer to the last list or null if there is no more page
    std::atomic<InsertLogTableEntry*> nextList;

    InsertTable table;
};

class InsertLogTable {
public:
    InsertLogTable(size_t capacity);

    ~InsertLogTable();

    const InsertLogEntry* get(uint64_t key, InsertLogTableEntry** headList = nullptr) const;

    InsertLogEntry* get(uint64_t key, InsertLogTableEntry** headList = nullptr) {
        return const_cast<InsertLogEntry*>(const_cast<const InsertLogTable*>(this)->get(key, headList));
    }

    bool insert(uint64_t key, InsertLogEntry* record, InsertLogTableEntry* headList);

    bool remove(uint64_t key, const InsertLogEntry* oldRecord, InsertLogTableEntry* tailList);

    InsertLogTableEntry* allocateHead();

    void truncate(InsertLogTableEntry* endList);

private:
    std::atomic<InsertLogTableEntry*> mHeadList;
};

} // namespace deltamain
} // namespace store
} // namespace tell
