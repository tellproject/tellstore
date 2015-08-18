#include "ColumnMapPage.hpp"
#include "deltamain/InsertMap.hpp"
#include "deltamain/Record.hpp"
#include "deltamain/Table.hpp"

#include <util/CuckooHash.hpp>

namespace tell {
namespace store {
namespace deltamain {

#include "ColumnMapUtils.in" // includes convenience functions for colum-layout

ColumnMapPage::ColumnMapPage(PageManager& pageManager, char* data, Table *table)
    : mPageManager(pageManager)
    , mTable(table)
    , mNullBitmapSize(getNullBitMapSize(table))
    , mNumColumns(table->getNumberOfFixedSizedFields() + table->getNumberOfVarSizedFields())
    , mFixedValuesSize(table->getFieldOffset(table->getNumberOfFixedSizedFields()) - table->getFieldOffset(0))
    , mData(data)
    , mRecordCount(*(reinterpret_cast<uint32_t*>(data)))
    , mFillPage(nullptr)
    , mFillPageVarOffsetPtr(nullptr)
    , mFillPageRecordCount(0)
    , mPageCleaningSummaries() {}

/**
 * efficient cleaning check on an entire page, returns a map of records that do need cleaning
 */
bool ColumnMapPage::needsCleaning(uint64_t lowestActiveVersion, InsertMap& insertMap) {

    // if there is already an entry in cleaning data, we have done the work already
    if (mPageCleaningSummaries.back().first == mData)
        return true;

    // do first quick path to detect whether the page needs cleaning at all
    bool needsCleaning = false;
    auto keyVersionPtr = getKeyAt(0, mData);        // this ptr gets always advance by 2 such that ptr[0] refers to key and ptr[1] to version
    auto newestPtr = reinterpret_cast<char**>(const_cast<char*>(getNewestPtrAt(0, mData, mRecordCount)));
    auto varLengthPtr = getVarsizedLenghtAt(0, mData, mRecordCount, mNullBitmapSize);
    auto keyVersionPtrEnd = keyVersionPtr + 2*mRecordCount;
    for (; !needsCleaning && keyVersionPtr < keyVersionPtrEnd; keyVersionPtr +=2, ++newestPtr, ++varLengthPtr) {
        if (keyVersionPtr[1] < lowestActiveVersion     // version below base version
                || *newestPtr                   // updates on this record
                || *varLengthPtr <= 0)          // deletions or reverts
            needsCleaning = true;
    }

    if (!needsCleaning)
        return false;

    // page needs cleaning, now let us gather the cleaning summary
    std::vector<std::pair<uint32_t, RecordCleaningInfo>> pageCleaningSummary;

    uint32_t startIndex = 0;
    keyVersionPtr = getKeyAt(startIndex, mData);
    newestPtr = reinterpret_cast<char**>(const_cast<char*>(getNewestPtrAt(startIndex, mData, mRecordCount)));
    varLengthPtr = getVarsizedLenghtAt(startIndex, mData, mRecordCount, mNullBitmapSize);
    for (; keyVersionPtr < keyVersionPtrEnd; keyVersionPtr +=2, ++newestPtr, ++varLengthPtr, ++startIndex) {
        pageCleaningSummary.emplace_back();
        pageCleaningSummary.back().first = startIndex;
        RecordCleaningInfo &recInfo = pageCleaningSummary.back().second;
        recInfo.newestPtr = *newestPtr;
        // iterate over all versions of this record and gather info
        for (; ; keyVersionPtr +=2, ++newestPtr, ++varLengthPtr, ++startIndex) {
            if (keyVersionPtr[1] >= lowestActiveVersion) {  //if version no longer valid, skip it
                if (*varLengthPtr == 0) {
                    // we have a delete record which means that we can delete the whole record
                    // iff everything else is below base version, otherwise we have to keep it
                    // and have to check insertMap
                    if (keyVersionPtr[0] == keyVersionPtr[2] && keyVersionPtr[3] >= lowestActiveVersion) {
                        // there are tuples above base version --> need to keep delete record and probe insertmap
                        recInfo.tupleCount++;
                        if (insertMap.count(*keyVersionPtr)) {
                            auto &queue = insertMap.find(*keyVersionPtr)->second;
                            for (auto iter = queue.begin(); iter != queue.end(); ++iter) {
                                CDMRecord rec (*iter);
                                //todo: use rec.collect(.) to iterate over all all records this insert involves
                            }
                        }
                    }
                }
                else if (*varLengthPtr > 0) {
                    recInfo.tupleCount++;
                    recInfo.totalVarSizedCount += (*varLengthPtr);
                }
                // else: we have a revert enty which we can simply ignore
            }
            if (keyVersionPtr[0] != keyVersionPtr[2]) break;  //loop exit condition
        }

        // iterate over updates of this record and add them as needed
        auto update = recInfo.newestPtr;
        while (update != nullptr) {
            CDMRecord rec (update);
            //todo: do the following without using logrecord directly
//            if (rec.isValidDataRecord() && rec.version() >= lowestActiveVersion) {
//                recInfo.tupleCount++;
//                recInfo.totalVarSizedCount += (rec.recordSize() - mFixedValuesSize);
//            }
//            update = const_cast<char *>(rec.getPrevious());
        }
    }

    // add pageCleaningSummary of this page to the collection of summaries
    mPageCleaningSummaries.emplace_back(mData, std::move(pageCleaningSummary));
    return false;
}

///**
// * efficient copy and compact algorithm for an MV record
// * returns true on success and false if there wasn't enough space in page
// */
//inline bool copyAndCompact(std::unordered_set<uint64_t> &cleaningMap,
//                           uint32_t *startIndex,
//                           const uint64_t *keyPtr,
//                           uint32_t *varHeapOffsetPtr,
//                           uint32_t *countPtr,
//                           const char *srcBasePtr,
//                           const char *destBasePtr,
//                           Table *table,
//                           const uint32_t numColumns,   //total number of columns (fixed and var)
//                           const uint32_t capacity,
//                           const size_t nullBitMapSize
//        )
//{
//    uint recordSize = 1;
//    uint totalVarLenghtSize = 0;
//    std::vector<uint32_t> varLengthOffsets;
//    if (cleaningMap.count(*keyPtr))
//    {
//        // (a) perform cleaning
//        // todo: implement
//    }
//    else
//    {
//        // (b) simply copy record
//        // compute size of record and var-heap consumption for copying
//        for (;; ++recordSize, keyPtr +=2)
//        {
//            varLengthOffsets.emplace_back(*varHeapOffsetPtr + totalVarLenghtSize);
//            totalVarLenghtSize += (*getVarsizedLenghtAt(*startIndex, srcBasePtr, capacity, nullBitMapSize));
//            if (keyPtr[0] <= keyPtr[2]) break;
//        }
//        if (((*countPtr) + recordSize > capacity) || ((*varHeapOffsetPtr) + totalVarLenghtSize > TELL_PAGE_SIZE))
//            return false;
//        // copy special columns
//        memcpy(const_cast<char *>(getKeyVersionPtrAt(*countPtr, destBasePtr)), getKeyVersionPtrAt(*startIndex, srcBasePtr), 16*recordSize);
//        // newest ptrs are all 0 therefore do need to copy
//        mempcpy(
//            const_cast<char *>(getNullBitMapAt(*countPtr, destBasePtr, capacity, nullBitMapSize)),
//            getNullBitMapAt(*startIndex, srcBasePtr, capacity, nullBitMapSize),
//            recordSize * nullBitMapSize);
//        memcpy(
//            const_cast<int32_t *>(getVarsizedLenghtAt(*countPtr, destBasePtr, capacity, nullBitMapSize)),
//            getVarsizedLenghtAt(*startIndex, srcBasePtr, capacity, nullBitMapSize),
//            recordSize * 4);

//        // copy fixed-sized and var-sized column data
//        for (uint col = 0; col < numColumns; ++col) {
//            memcpy(
//                getColumnNAt(table, col, *countPtr, destBasePtr, capacity, nullBitMapSize),
//                getColumnNAt(table, col, *startIndex, srcBasePtr, capacity, nullBitMapSize),
//                recordSize * table->getFieldSize(col));
//        }

//        // copy var-sized heap data
//        memcpy(
//            const_cast<char *>(destBasePtr + *varHeapOffsetPtr),
//            srcBasePtr + *(reinterpret_cast<uint32_t*>(getColumnNAt(table, table->getNumberOfFixedSizedFields(), *startIndex, srcBasePtr, capacity, nullBitMapSize))),
//            totalVarLenghtSize);

//        // adjust var-sized offsets
//        for (uint col = table->getNumberOfFixedSizedFields(); col < numColumns; ++col) {
//            auto varLenghtptrCountPtr = getColumnNAt(table, col, *startIndex, srcBasePtr, capacity, nullBitMapSize);
//            for (uint i = 0; i < recordSize; ++i, varLenghtptrCountPtr += 2) {
//                *varLenghtptrCountPtr = varLengthOffsets[i];
//                varLengthOffsets[i] += *(reinterpret_cast<const uint32_t *>(destBasePtr + *(reinterpret_cast<uint32_t*>(varLengthOffsets[i]))));
//            }
//        }

//        // adjust pointers in hash map
//        //todo: implement
//    }

//    (*startIndex) += recordSize;
//    (*countPtr) += recordSize;
//    (*varHeapOffsetPtr) += totalVarLenghtSize;
//    return true;
//}

char* ColumnMapPage::gc(uint64_t lowestActiveVersion,
        InsertMap& insertMap,
        bool& done,
        Modifier& hashTable)
{
    (mTable->getPageCapacity());

    //todo: go to this whole code and make it work in new table-gc method

//    auto cleaningMap = needCleaning(lowestActiveVersion, insertMap);
//    if (cleaningMap.size() == 0) {
//        // we are done - no cleaning needed for this page
//        done = true;
//        return mData;
//    }

//    // At this point we know that we will need to clean the page
//    uint32_t *varHeapOffsetPtr = (reinterpret_cast<uint32_t*>(fillPage));
//    if ((*varHeapOffsetPtr) == 0)
//        (*varHeapOffsetPtr) = reinterpret_cast<uint64_t>(getColumnNAt(mTable, numColumns, 0, fillPage, capacity, nullBitMapSize))
//            - reinterpret_cast<uint64_t>(fillPage);
//    uint32_t *countPtr = (reinterpret_cast<uint32_t*>(fillPage+4));
//    char* res = (*countPtr) == 0 ? fillPage : nullptr;   //nullptr means that the fillpage was already added to the page list at another iteration of gc
//    // now we also know that we will have to recycle the current
//    // read only page
//    markCurrentForDeletion();   //TODO: are we sure that his actually only happens to a page once? What if there are MANY updates?!

//    // now we need to iterate over the page and simply copy or clean records as necessary
//    auto keyPtr = getKeyAt(mStartIndex, mData);
//    for (auto keyPtrEnd = keyPtr + 2*capacity; keyPtr < keyPtrEnd; keyPtr +=2) {
//        if(!copyAndCompact(cleaningMap, &mStartIndex, keyPtr, varHeapOffsetPtr, countPtr, mData, fillPage, mTable, numColumns, capacity, nullBitMapSize)) {
//            // The current fillPage is full
//            // In this case we will either allocate a new fillPage (if
//            // the old one got inserted before), or we will return to
//            // indicate that we need a new fillPage
//            // in either case, we have to seal the fillpage (by setting capacity correclty)
//            *(reinterpret_cast<uint32_t *>(fillPage)) = capacity;
//            if (res) {
//                done = false;
//                return res;
//            } else {
//                // In this case the fillPage is already in the pageList.
//                // We can safely allocate a new page and allocate this one.
//                fillPage = reinterpret_cast<char*>(mPageManager.alloc());
//                res = fillPage;
//                // now we can try again
//                continue;
//            }
//        }
//    }
//    // we are done. It might now be, that this page has some free space left
//    fillWithInserts(lowestActiveVersion, insertMap, fillPage, hashTable);
//    done = true;
//    return res;
    return nullptr;
}

char *ColumnMapPage::fillWithInserts(uint64_t lowestActiveVersion, InsertMap& insertMap, Modifier& hashTable)
{
    //TODO: implement
    return nullptr;
}

} // namespace deltamain
} // namespace store
} // namespace tell

