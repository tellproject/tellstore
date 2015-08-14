#include "ColumnMapPage.hpp"
#include "deltamain/InsertMap.hpp"
#include "deltamain/Record.hpp"
#include "deltamain/Table.hpp"

#include <util/CuckooHash.hpp>

#include <unordered_set>
#include <vector>

namespace tell {
namespace store {
namespace deltamain {

#include "ColumnMapUtils.in" // includes convenience functions for colum-layout

/**
 * efficient cleaning check on an entire page, returns a map of records that do need cleaning
 */
inline std::unordered_set<uint64_t> needCleaning(
                          uint64_t lowestActiveVersion,
                          InsertMap& insertMap,
                          const uint32_t startIndex,
                          const char * basePtr,
                          const uint32_t capacity,
                          const uint32_t count,
                          const size_t nullBitMapSize) {

    std::unordered_set<uint64_t> res;

    // check whether there are some versions below base versions and whether the key appears in the insert map
    auto keyPtr = getKeyAt(startIndex, basePtr);
    for (auto keyPtrEnd = keyPtr + 2*count; keyPtr < keyPtrEnd; keyPtr +=2) {
        if (keyPtr[1] < lowestActiveVersion || insertMap.count(keyPtr[0]))
            res.emplace(keyPtr[0]);
    }

    // check whether there were updates
    uint64_t key = startIndex;
    keyPtr = reinterpret_cast<const uint64_t *>(getNewestPtrAt(startIndex, basePtr, capacity));
    for (auto keyPtrEnd = keyPtr + count; keyPtr < keyPtrEnd; ++keyPtr, ++key) {
        if (*keyPtr)
            res.emplace(key);
    }

    // check whether there were deletions or reverts
    key = startIndex;
    auto varlengthPtr = getVarsizedLenghtAt(startIndex, basePtr, capacity, nullBitMapSize);
    for (auto varlengthPtrEnd = varlengthPtr + count; varlengthPtr < varlengthPtrEnd; ++varlengthPtr, ++key) {
        if (*varlengthPtr <= 0)
            res.emplace(key);
    }

    return res;
}

/**
 * efficient copy and compact algorithm for an MV record
 * returns true on success and false if there wasn't enough space in page
 */
inline bool copyAndCompact(std::unordered_set<uint64_t> &cleaningMap,
                           uint32_t *startIndex,
                           const uint64_t *keyPtr,
                           uint32_t *varHeapOffsetPtr,
                           uint32_t *countPtr,
                           const char *srcBasePtr,
                           const char *destBasePtr,
                           Table *table,
                           const uint32_t numColumns,   //total number of columns (fixed and var)
                           const uint32_t capacity,
                           const size_t nullBitMapSize
        )
{
    uint recordSize = 1;
    uint totalVarLenghtSize = 0;
    std::vector<uint32_t> varLengthOffsets;
    if (cleaningMap.count(*keyPtr))
    {
        // (a) perform cleaning
        // todo: implement
    }
    else
    {
        // (b) simply copy record
        // compute size of record and var-heap consumption for copying
        for (;; ++recordSize, keyPtr +=2)
        {
            varLengthOffsets.emplace_back(*varHeapOffsetPtr + totalVarLenghtSize);
            totalVarLenghtSize =+ (*getVarsizedLenghtAt(*startIndex, srcBasePtr, capacity, nullBitMapSize));
            if (keyPtr[0] <= keyPtr[2]) break;
        }
        if (((*countPtr) + recordSize > capacity) || ((*varHeapOffsetPtr) + totalVarLenghtSize > TELL_PAGE_SIZE))
            return false;
        // copy special columns
        memcpy(const_cast<char *>(getKeyVersionPtrAt(*countPtr, destBasePtr)), getKeyVersionPtrAt(*startIndex, srcBasePtr), 16*recordSize);
        // newest ptrs are all 0 therefore do need to copy
        mempcpy(
            const_cast<char *>(getNullBitMapAt(*countPtr, destBasePtr, capacity, nullBitMapSize)),
            getNullBitMapAt(*startIndex, srcBasePtr, capacity, nullBitMapSize),
            recordSize * nullBitMapSize);
        memcpy(
            const_cast<uint32_t *>(getVarsizedLenghtAt(*countPtr, destBasePtr, capacity, nullBitMapSize)),
            getVarsizedLenghtAt(*startIndex, srcBasePtr, capacity, nullBitMapSize),
            recordSize * 4);

        // copy fixed-sized and var-sized column data
        for (uint col = 0; col < numColumns; ++col) {
            memcpy(
                getColumnNAt(table, col, *countPtr, destBasePtr, capacity, nullBitMapSize),
                getColumnNAt(table, col, *startIndex, srcBasePtr, capacity, nullBitMapSize),
                recordSize * table->getFieldSize(col));
        }

        // copy var-sized heap data
        memcpy(
            const_cast<char *>(destBasePtr + *varHeapOffsetPtr),
            srcBasePtr + *(reinterpret_cast<uint32_t*>(getColumnNAt(table, table->getNumberOfFixedSizedFields(), *startIndex, srcBasePtr, capacity, nullBitMapSize))),
            totalVarLenghtSize);

        // adjust var-sized offsets
        for (uint col = table->getNumberOfFixedSizedFields(); col < numColumns; ++col) {
            auto varLenghtptrCountPtr = getColumnNAt(table, col, *startIndex, srcBasePtr, capacity, nullBitMapSize);
            for (uint i = 0; i < recordSize; ++i, varLenghtptrCountPtr += 2) {
                *varLenghtptrCountPtr = varLengthOffsets[i];
                varLengthOffsets[i] += *(reinterpret_cast<const uint32_t *>(destBasePtr + *(reinterpret_cast<uint32_t*>(varLengthOffsets[i]))));
            }
        }

        // adjust pointers in hash map
        //todo: implement
    }

    (*startIndex) += recordSize;
    (*countPtr) += recordSize;
    (*varHeapOffsetPtr) += totalVarLenghtSize;
    return true;
}

//TODO: question: this implementation relies on the fact that fresh pages are meset to 0s. Is that actually the case?
//TODO: question: will mStartIndex be valid for the whole GC phase? Can it happen that the page is called later again
// by GC, but with a newly generated Page object (and mStartIndex reset to 0)?
char* ColumnMapPage::gc(
        uint64_t lowestActiveVersion,
        InsertMap& insertMap,
        char*& fillPage,
        bool& done,
        Modifier& hashTable)
{
    // general base knowedge
    auto capacity = mTable->getPageCapacity();
    auto nullBitMapSize = getNullBitMapSize(mTable);
    auto numColumns = mTable->getNumberOfFixedSizedFields() + mTable->getNumberOfVarSizedFields();
    auto srcCount = *(reinterpret_cast<uint32_t*>(mTable+4));

    auto cleaningMap = needCleaning(lowestActiveVersion, insertMap, mStartIndex, mData, capacity, srcCount, nullBitMapSize);
    if (cleaningMap.size() == 0) {
        // we are done - no cleaning needed for this page
        done = true;
        return mData;
    }

    // At this point we know that we will need to clean the page
    uint32_t *varHeapOffsetPtr = (reinterpret_cast<uint32_t*>(fillPage));
    if ((*varHeapOffsetPtr) == 0)
        (*varHeapOffsetPtr) = reinterpret_cast<uint64_t>(getColumnNAt(mTable, numColumns, 0, fillPage, capacity, nullBitMapSize))
            - reinterpret_cast<uint64_t>(fillPage);
    uint32_t *countPtr = (reinterpret_cast<uint32_t*>(fillPage+4));
    char* res = (*countPtr) == 0 ? fillPage : nullptr;   //nullptr means that the fillpage was already added to the page list at another iteration of gc
    // now we also know that we will have to recycle the current
    // read only page
    markCurrentForDeletion();   //TODO: are we sure that his actually only happens to a page once? What if there are MANY updates?!

    // now we need to iterate over the page and simply copy or clean records as necessary
    auto keyPtr = getKeyAt(mStartIndex, mData);
    for (auto keyPtrEnd = keyPtr + 2*capacity; keyPtr < keyPtrEnd; keyPtr +=2) {
        if(!copyAndCompact(cleaningMap, &mStartIndex, keyPtr, varHeapOffsetPtr, countPtr, mData, fillPage, mTable, numColumns, capacity, nullBitMapSize)) {
            // The current fillPage is full
            // In this case we will either allocate a new fillPage (if
            // the old one got inserted before), or we will return to
            // indicate that we need a new fillPage
            // in either case, we have to seal the fillpage (by setting capacity correclty)
            *(reinterpret_cast<uint32_t *>(fillPage)) = capacity;
            if (res) {
                done = false;
                return res;
            } else {
                // In this case the fillPage is already in the pageList.
                // We can safely allocate a new page and allocate this one.
                fillPage = reinterpret_cast<char*>(mPageManager.alloc());
                res = fillPage;
                // now we can try again
                continue;
            }
        }
    }
    // we are done. It might now be, that this page has some free space left
    fillWithInserts(lowestActiveVersion, insertMap, fillPage, hashTable);
    done = true;
    return res;
}

void ColumnMapPage::fillWithInserts(uint64_t lowestActiveVersion, InsertMap& insertMap, char*& fillPage, Modifier& hashTable)
{
    //TODO: implement
}

} // namespace deltamain
} // namespace store
} // namespace tell

