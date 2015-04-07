#pragma once

#include <util/functional.hpp>

#include <atomic>
#include <cstdint>

namespace tell {
namespace store {
namespace logstructured {

/**
 * @brief Lock-Free Open-Addressing hash table for associating a pointer with a <tableId, keyId> pair
 *
 * Pointers must be 8 byte aligned and neither the tableId nor the keyId are allowed to be 0.
 */
class OpenAddressingTable {
public:
    OpenAddressingTable(size_t capacity);

    ~OpenAddressingTable();

    /**
     * @brief Looks up the element in the hash table
     *
     * @param table The table ID of the entry
     * @param key The key ID of the entry
     * @return The associated data pointer or nullptr if the element did not exist
     */
    void* get(uint64_t table, uint64_t key);

    /**
     * @brief Tries to insert the element into the hash table
     *
     * @param table The table ID of the entry
     * @param key The key ID of the entry
     * @param data The new data pointer
     * @return True if the insert was successful, false if the element already exists in the hash table
     */
    bool insert(uint64_t table, uint64_t key, void* data);

    /**
     * @brief Tries to update an already existing element from the old pointer to the new pointer
     *
     * Searches for the element and if it exists tries to update the entries pointer from oldData to newData.
     *
     * @param table The table ID of the entry
     * @param key The key ID of the entry
     * @param oldData The previous data pointer
     * @param newData The new data pointer
     * @return True if the update was successful, false if the element did not exist or the data pointer has changed
     */
    bool update(uint64_t table, uint64_t key, void* oldData, void* newData);

    /**
     * @brief Erases an already existing element from the hash table
     *
     * @param table The table ID of the entry
     * @param key The key ID of the entry
     * @param oldData The previous data pointer
     * @return True if the element did not exist or was successfully removed, false if the data pointer has changed
     */
    bool erase(uint64_t table, uint64_t key, void* oldData);

private:
    /**
     * @brief Struct holding the bucket data
     */
    struct Entry {
        Entry()
                : tableId(0x0u),
                  keyId(0x0u),
                  ptr(nullptr) {
        }

        std::atomic<uint64_t> tableId;
        std::atomic<uint64_t> keyId;
        std::atomic<void*> ptr;
    };

    /**
     * @brief Helper function to erase the entries data and set the entry to deleted
     */
    static void deleteEntry(Entry& entry);

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
    uint64_t calculateHash(uint64_t table, uint64_t key);

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
     * @return Whether other inserts conflict with the current one
     */
    bool hasInsertConflict(size_t hash, size_t pos, uint64_t table, uint64_t key);

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
    T execOnElement(uint64_t table, uint64_t key, T notFound, F fun);

    size_t mCapacity;

    Entry* mBuckets;

    cuckoo_hash_function mTableHash;
    cuckoo_hash_function mKeyHash;
};


} // namespace logstructured
} // namespace store
} // namespace tell
