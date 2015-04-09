#include "Table.hpp"

#include <util/OpenAddressingHash.hpp>
#include <util/PageManager.hpp>

namespace tell {
namespace store {
namespace logstructured {

Table::Table(PageManager& pageManager, HashTable& hashMap, const Schema& schema, uint64_t tableId)
    : mPageManager(pageManager),
      mHashMap(hashMap),
      mSchema(schema),
      mTableId(tableId),
      mLog(mPageManager) {
}

bool Table::get(uint64_t key, size_t& size, const char*& data, const SnapshotDescriptor& snapshot, bool& isNewest)
        const {
    // TODO Implement
    return false;
}

void Table::insert(uint64_t key, const GenericTuple& tuple, const SnapshotDescriptor& snapshot,
        bool* succeeded /* = nullptr */) {
    // TODO Implement
}

void Table::insert(uint64_t key, size_t size, const char* data, const SnapshotDescriptor& snapshot,
        bool* succeeded /* = nullptr */) {
    // TODO Implement
}

bool Table::update(uint64_t key, size_t size, const char* data, const SnapshotDescriptor& snapshot) {
    // TODO Implement
    return false;
}

bool Table::remove(uint64_t key, const SnapshotDescriptor& snapshot) {
    // TODO Implement
    return false;
}

void Table::runGC(uint64_t minVersion) {
    // TODO Implement
}

} // namespace logstructured
} // namespace store
} // namespace tell
