#pragma once

#include "functional.hpp"

#include <atomic>
#include <cstdint>

namespace tell {
namespace store {

/**
 * @brief Lock-Free Open-Addressing hash table for associating a pointer with a <table, key> pair
 *
 * The hash table is designed to store pointers to a record for a given table and key and as such has a few limitations:
 *
 * * A table ID of 0 is not allowed
 * * Pointers have to be unique, i.e. two different <table, key> pairs are not allowed to map to the same pointer
 * * Pointers must be 8 byte aligned
 * * Interactions with the table must be made while in an active epoch which manages all pointers
 *
 * These limitations are required to prevent certain ABA problems from arising.
 */
class OpenAddressingTable {
public:
    OpenAddressingTable(size_t capacity);

    ~OpenAddressingTable();

    size_t capacity() const {
        return mCapacity;
    }

    /**
     * @brief Looks up the element in the hash table
     *
     * @param table The table ID of the entry
     * @param key The key ID of the entry
     * @return The associated data pointer or nullptr if the element did not exist
     */
    void* get(uint64_t table, uint64_t key) {
        return const_cast<void*>(const_cast<const OpenAddressingTable*>(this)->get(table, key));
    }

    /**
     * @brief Looks up the element in the hash table
     *
     * @param table The table ID of the entry
     * @param key The key ID of the entry
     * @return The associated data pointer or nullptr if the element did not exist
     */
    const void* get(uint64_t table, uint64_t key) const;

    /**
     * @brief Invokes the provided function for every valid tuple in the range between start and end bucket
     *
     * @param start Offset to the start bucket
     * @param end Offset to the end bucket (end bucket will not be included)
     */
    template <typename F>
    void forEach(size_t start, size_t end, F fun) {
        forEachImpl<F, void*>(start, end, std::move(fun));
    }

    /**
     * @brief Invokes the provided function for every valid tuple in the range between start and end bucket
     *
     * @param start Offset to the start bucket
     * @param end Offset to the end bucket (end bucket will not be included)
     */
    template <typename F>
    void forEach(size_t start, size_t end, F fun) const {
        forEachImpl<F, const void*>(start, end, std::move(fun));
    }

    /**
     * @brief Tries to insert the element into the hash table
     *
     * @param table The table ID of the entry
     * @param key The key ID of the entry
     * @param data The new data pointer
     * @param actualData Pointer to the element which caused the conflict
     * @return True if the insert was successful, false if the element already exists in the hash table
     */
    bool insert(uint64_t table, uint64_t key, void* data, void** actualData = nullptr);

    /**
     * @brief Tries to update an already existing element from the old pointer to the new pointer
     *
     * Searches for the element and if it exists tries to update the entries pointer from oldData to newData.
     *
     * @param table The table ID of the entry
     * @param key The key ID of the entry
     * @param oldData The previous data pointer
     * @param newData The new data pointer
     * @param actualData Pointer to the element which caused the conflict
     * @return True if the update was successful, false if the element did not exist or the data pointer has changed
     */
    bool update(uint64_t table, uint64_t key, const void* oldData, void* newData, void** actualData = nullptr);

    /**
     * @brief Erases an already existing element from the hash table
     *
     * @param table The table ID of the entry
     * @param key The key ID of the entry
     * @param oldData The previous data pointer
     * @param actualData Pointer to the element which caused the conflict
     * @return True if the element did not exist or was successfully removed, false if the data pointer has changed
     */
    bool erase(uint64_t table, uint64_t key, const void* oldData, void** actualData = nullptr);

private:
    /**
     * @brief Struct holding the bucket data
     */
    struct Entry {
        Entry()
                : tableId(0x0u),
                  keyId(0x0u),
                  ptr(0x0u) {
        }

        std::atomic<uint64_t> tableId;
        std::atomic<uint64_t> keyId;
        std::atomic<uintptr_t> ptr;
    };

    /**
     * @brief The potential states a Entry pointer can be tagged with
     */
    enum class EntryMarker : uintptr_t {
        /// The entry is still free
        FREE = 0x0u,

        /// The entry is deleted and can be reused (actual pointer will be null)
        DELETED = 0x1u,

        /// The entry is invalid and must not be touched (actual pointer will be null)
        INVALID = (0x1u << 1),

        /// Data is currently written to the entry
        INSERTING = (0x1u << 2),
    };

    /**
     * @brief Bitmask to retrieve the pure pointer from a marked Entry pointer
     */
    static constexpr uintptr_t POINTER_MASK = (UINTPTR_MAX << 3);

    /**
     * @brief Bitmask to retrieve the pure markers from a marked Entry pointer
     */
    static constexpr uintptr_t MARKER_MASK = ~POINTER_MASK;

    /**
     * @brief Helper function to erase the entries data and set the entry to deleted
     */
    static void deleteEntry(Entry& entry);

    /**
     * @brief Helper function setting the actual data pointer when the modification of the given entry failed
     *
     * @param actualData The pointer where to store the actual data
     * @param entry Entry that the modification failed on
     * @param ptr The actual pointer of the entry
     */
    static void setActualData(uint64_t table, uint64_t key, void** actualData, const Entry& entry, uintptr_t ptr);

    /**
     * @brief Calculates a 64 bit hash from the table and key ID
     *
     * Hashes both table and key independently using the Cuckoo hash function and combines the two resulting hashes into
     * one.
     *
     * @param table The table ID of the entry
     * @param key The key ID of the entry
     * @return The 64 bit hash of both table and key
     */
    uint64_t calculateHash(uint64_t table, uint64_t key) const;

    /**
     * @brief Checks for any conflicting inserts happening on the same table and key
     *
     * Scans the overflow buffer and tries to abort any concurrent inserts that are trying to insert in a bucket
     * following the current bucket.
     *
     * @param hash The hash value of table and key
     * @param pos The position the current element will be inserted
     * @param table The table ID of the entry
     * @param key The key ID of the entry
     * @param actualData Pointer to the element which caused the conflict
     * @return Whether other inserts conflict with the current one
     */
    bool hasInsertConflict(size_t hash, size_t pos, uint64_t table, uint64_t key, void** actualData);

    /**
     * @brief Searches the hash table for the element with table and key and executes the function on the element
     *
     * @param table The table ID of the entry
     * @param key The key ID of the entry
     * @param notFound The value to return when the element was not found
     * @param fun The function to be executed on the target element, interface must match T fun(Entry&, void* ptr)
     * @return The return value of fun or notFound if the element was not found
     */
    template <typename T, typename F>
    T execOnElement(uint64_t table, uint64_t key, T notFound, F fun) const;

    template <typename T, typename F>
    T execOnElement(uint64_t table, uint64_t key, T notFound, F fun);

    /**
     * @brief Invokes the provided function for every valid tuple in the range between start and end bucket
     *
     * @param start Offset to the start bucket
     * @param end Offset to the end bucket (end bucket will not be included)
     */
    template <typename F, typename T>
    void forEachImpl(size_t start, size_t end, F fun) const;

    size_t mCapacity;

    Entry* mBuckets;

    cuckoo_hash_function mTableHash;
    cuckoo_hash_function mKeyHash;
};

template <typename F, typename T>
void OpenAddressingTable::forEachImpl(size_t start, size_t end, F fun) const {
    if (start > end || end > mCapacity) {
        throw std::out_of_range("Invalid for-each bounds");
    }

    auto entryIt = mBuckets + start;
    auto entryEnd = mBuckets + end;
    while (entryIt != entryEnd) {
        auto ptr = entryIt->ptr.load();

        // Check if pointer is marked as free, deleted, inserting or invalid - Skip bucket if it is
        if (ptr == 0x0u || (ptr & MARKER_MASK) != 0x0u) {
            ++entryIt;
            continue;
        }

        auto t = entryIt->tableId.load();
        auto k = entryIt->keyId.load();

        // Check if the pointer changed - Reload variables if it has
        // Needed to ensure the loaded ptr, table and key are consistent and were not changed by other threads in the
        // meantime. This does not suffer from the ABA problem as the table requires unique pointers and access through
        // an epoch.
        if (entryIt->ptr.load() != ptr) {
            continue;
        }

        fun(t, k, reinterpret_cast<T>(ptr & POINTER_MASK));
        ++entryIt;
    }
}

} // namespace store
} // namespace tell
