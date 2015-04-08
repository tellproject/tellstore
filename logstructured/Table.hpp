#pragma once

#include <util/Log.hpp>
#include <util/Record.hpp>
#include <util/NonCopyable.hpp>

#include <cstdint>

namespace tell {
namespace store {

class OpenAddressingTable;
class PageManager;

namespace logstructured {

/**
 * @brief A table using a Log-Structured Memory approach as its data store
 */
class Table : NonCopyable, NonMovable {
public:
    using HashTable = OpenAddressingTable;

    Table(PageManager& pageManager, HashTable& hashMap, const Schema& schema, uint64_t tableId);

    /**
     * @brief Reads a tuple from the table
     *
     * @param key Key of the tuple to retrieve
     * @param size Reference to the tuple's size
     * @param data Reference to the tuple's data pointer
     * @param snapshot Descriptor containing the versions allowed to read
     * @param isNewest Whether the returned tuple contains the newest version written
     * @return Whether the tuple was found
     */
    bool get(uint64_t key, size_t& size, const char*& data, const SnapshotDescriptor& snapshot, bool& isNewest) const;

    /**
     * @brief Inserts a tuple into the table
     *
     * @param key Key of the tuple to insert
     * @param tuple The tuple to insert
     * @param snapshot Descriptor containing the version to write
     * @param succeeded Whether the tuple was inserted successfully
     */
    void insert(uint64_t key, const GenericTuple& tuple, const SnapshotDescriptor& snapshot, bool* succeeded = nullptr);

    /**
     * @brief Inserts a tuple into the table
     *
     * @param key Key of the tuple to insert
     * @param size Size of the tuple to insert
     * @param data Pointer to the data of the tuple to insert
     * @param snapshot Descriptor containing the version to write
     * @param succeeded Whether the tuple was inserted successfully
     */
    void insert(uint64_t key, size_t size, const char* data, const SnapshotDescriptor& snapshot,
            bool* succeeded = nullptr);

    /**
     * @brief Updates an already existing tuple in the table
     *
     * @param key Key of the tuple to update
     * @param size Size of the updated tuple
     * @param data Pointer to the data of the updated tuple
     * @param snapshot Descriptor containing the version to write
     * @return Whether the tuple was updated successfully
     */
    bool update(uint64_t key, size_t size, const char* data, const SnapshotDescriptor& snapshot);

    /**
     * @brief Removes an already existing tuple from the table
     *
     * @param key Key of the tuple to remove
     * @param snapshot Descriptor containing the version to remove
     * @return Whether the tuple was removed successfully
     */
    bool remove(uint64_t key, const SnapshotDescriptor& snapshot);

    /**
     * @brief Starts a garbage collection run
     *
     * @param minVersion Minimum version of the tuples to keep
     */
    void runGC(uint64_t minVersion);

private:
    PageManager& mPageManager;
    HashTable& mHashMap;
    Schema mSchema;
    uint64_t mTableId;

    Log<UnorderedLogImpl> mLog;
};

} // namespace logstructured
} // namespace store
} // namespace tell
