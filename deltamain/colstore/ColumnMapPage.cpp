#include "ColumnMapPage.hpp"
#include "deltamain/InsertMap.hpp"
#include "deltamain/Record.hpp"
#include "deltamain/Table.hpp"

#include <util/CuckooHash.hpp>

namespace tell {
namespace store {
namespace deltamain {

#include "ColumnMapUtils.in" // includes convenience functions for colum-layout

/**
 * efficient cleaning check on an entire page
 */
inline bool needsCleaning(const char *data, uint64_t lowestActiveVersion, InsertMap& insertMap, const Table* table, const char * basePtr, const uint32_t capacity, const size_t nullBitMapSize) {

    // check whether there are some versions below base versions and whether the key appears in the insert map
    uint64_t *keyPtr = const_cast<uint64_t *>(getKeyAt(0, basePtr));
    for (auto keyPtrEnd = keyPtr + 2*capacity; keyPtr < keyPtrEnd; keyPtr +=2) {
        if (keyPtr[1] < lowestActiveVersion) return true;
        if (insertMap.count(keyPtr[0])) return true;
    }

    // check whether there were updates (newestptrs start right after key-version column)
    for (auto keyPtrEnd = keyPtr + capacity; keyPtr < keyPtrEnd; ++keyPtr) {
        if (*keyPtr) return true;
    }

    // check whether there were deletions or reverts
    int32_t *varlengthPtr = const_cast<int32_t *>(getVarsizedLenghtAt(0, basePtr, capacity, nullBitMapSize));
    for (auto varlengthPtrEnd = varlengthPtr + capacity; varlengthPtr < varlengthPtrEnd; ++varlengthPtr) {
        if (*varlengthPtr <= 0) return true;
    }

    return false;
}

char* ColumnMapPage::gc(
        uint64_t lowestActiveVersion,
        InsertMap& insertMap,
        char*& fillPage,
        bool& done,
        Modifier& hashTable)
{
    COMPUTE_BASE_KNOWLEDGE(mData+8, mTable)
    auto nullBitMapSize = getNullBitMapSize(mTable);
    capacity = mTable->getPageCapacity();       // the capacity needs to be adjusted because it is not set in the page yet

    if (needsCleaning(mData, lowestActiveVersion, insertMap, mTable, basePtr, capacity, nullBitMapSize)) {
        // we are done - no cleaning needed for this page
        done = true;
        return mData;
    }

    // At this point we know that we will need to clean the page
    uint32_t *countPtr = (reinterpret_cast<uint32_t*>(mData+4));
    char* res = (*countPtr) == 0 ? fillPage : nullptr;   //nullptr means that the fillpage was already added to the page list at the last iteration of gc
    // now we also know that we will have to recycle the current
    // read only page
    markCurrentForDeletion();   //TODO: are we sure that his actually only happens to a page once? What if there are MANY updates?!

    // now we need to iterate over the page
    auto keyPtr = getKeyAt(0, basePtr);

    // continue here...

    return nullptr;
}

void ColumnMapPage::fillWithInserts(uint64_t lowestActiveVersion, InsertMap& insertMap, char*& fillPage, Modifier& hashTable)
{

}

} // namespace deltamain
} // namespace store
} // namespace tell

