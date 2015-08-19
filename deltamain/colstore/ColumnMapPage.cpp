#include "ColumnMapPage.hpp"
#include "deltamain/InsertMap.hpp"
#include "deltamain/Record.hpp"
#include "deltamain/Table.hpp"

#include <util/CuckooHash.hpp>

namespace tell {
namespace store {
namespace deltamain {

#include "ColumnMapUtils.in.cpp" // includes convenience functions for colum-layout

ColumnMapPage::ColumnMapPage(PageManager& pageManager, char* data, Table *table)
    : mPageManager(pageManager)
//    , mTable(table)
    , mNullBitmapSize(getNullBitMapSize(table))
    , mNumColumns(table->getNumberOfFixedSizedFields() + table->getNumberOfVarSizedFields())
    , mFixedValuesSize(table->getFieldOffset(table->getNumberOfFixedSizedFields()) - table->getFieldOffset(0))
    , mData(data)
    , mRecordCount(0)
    , mFillPage(nullptr)
    , mFillPageVarOffsetPtr(nullptr)
    , mFillPageRecordCount(0)
    , mPageCleaningSummaries() {}

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
        if (keyVersionPtr[1] < lowestActiveVersion                              // version below base version
                || *newestPtr                                                   // updates on this record
                || *varLengthPtr < 0                                            // reverts
                || (*varLengthPtr == 0 && insertMap.count(keyVersionPtr[0])))   // deletes need cleaning if they have consequent inserts (or if they have versions below base version which will be checked in the next loop iteration)
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
        auto &newest = recInfo.newestPtr;
        auto &versionMap = recInfo.versionMap;
        newest = *newestPtr;
        bool safelyDiscard = (newest != nullptr);    // if safe is true, I can discard all versions below base version (if not, I have to keep the youngest)
        // iterate over all versions of this record and gather info
        for (; ; keyVersionPtr +=2, ++newestPtr, ++varLengthPtr, ++startIndex) {
            if (!safelyDiscard || keyVersionPtr[1] >= lowestActiveVersion) {  //if version no longer valid, skip it
                if (*varLengthPtr == 0) {
                    // we have a delete record which means that we can delete the whole record
                    // iff everything else is below base version, otherwise we have to keep it
                    if (keyVersionPtr[0] == keyVersionPtr[2] && keyVersionPtr[3] >= lowestActiveVersion) {
                        // there are tuples above base version --> need to keep delete record and probe insertmap
                        versionMap.insert(std::make_pair(keyVersionPtr[1], impl::VersionHolder
                                {reinterpret_cast<char*>(const_cast<uint64_t*>(keyVersionPtr))+2,
                                RecordType::MULTI_VERSION_RECORD,
                                0,
                                nullptr}));
                    }
                    safelyDiscard = true;
                }
                else if (*varLengthPtr > 0) {
                    versionMap.insert(std::make_pair(keyVersionPtr[1], impl::VersionHolder
                            {reinterpret_cast<char*>(const_cast<uint64_t*>(keyVersionPtr))+2,
                            RecordType::MULTI_VERSION_RECORD,
                            (*varLengthPtr) + mFixedValuesSize,
                            nullptr}));
                    safelyDiscard = true;
                }
                // else: we have a revert entry which we can simply ignore
            }
            if (keyVersionPtr[0] != keyVersionPtr[2]) break;  //loop exit condition
        }

        // if this record has updates or inserts add them as needed
        bool newestIsDelete = true, allVersionsInvalid;
        if (newest)
        {
            CDMRecord rec (newest);
            rec.collect(versionMap, newestIsDelete, allVersionsInvalid);
        }
        if (insertMap.count(keyVersionPtr[0])) {
            auto insertMapIter = insertMap.find(keyVersionPtr[0]);
            auto &queue = insertMapIter->second;
            for (auto queueIter = queue.begin(); queueIter != queue.end(); ++queueIter) {
                if (!newestIsDelete)
                {
                    LOG_ERROR("You are not supposed to call this on a ColMapMVRecord");
                    std::terminate();
                }
                CDMRecord rec (*queueIter);
                rec.collect(versionMap, newestIsDelete, allVersionsInvalid);
                if (newestIsDelete || allVersionsInvalid) {
                    if (queue.empty()) {
                        insertMap.erase(insertMapIter);
                        break;
                    }
                } else {
                    // in this case we do not need to check further for inserts
                    break;
                }
            }
        }

        // prune no longer used versions of versionMap and save newest pointer to the value that youngest insert pointed to
        // (which is equal to the address of the highest valid version or NULL if that's an insert)
        auto firstValidVersion = versionMap.lower_bound(lowestActiveVersion);
        if (firstValidVersion == versionMap.end()) {
            --firstValidVersion;
        }
        versionMap.erase(versionMap.begin(), firstValidVersion);
        auto hightsVersionRecord = versionMap.rbegin();
        if (hightsVersionRecord != versionMap.rend())
            newest = (hightsVersionRecord->second.type == RecordType::LOG_INSERT ?
                    nullptr :
                    const_cast<char *>(hightsVersionRecord->second.record));

        // add information from versionMap to recordInfo
        for (auto iter = versionMap.begin(); iter!=versionMap.end(); ++iter)
        {
            recInfo.tupleCount++;
            if (iter->second.type != RecordType::LOG_DELETE && iter->second.size != 0)  // size is 0 for deletes in MVRecord
                recInfo.totalVarSizedCount += iter->second.size - mFixedValuesSize;
        }
    }

    // add pageCleaningSummary of this page to the collection of summaries
    mPageCleaningSummaries.emplace_back(mData, std::move(pageCleaningSummary));
    return false;
}

bool ColumnMapPage::copyAndCompact(uint64_t lowestActiveVersion,
                    InsertMap& insertMap,
                    Modifier& hashTable
        )
{
    //TODO: implement
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
    return true;
}

char* ColumnMapPage::gc(uint64_t lowestActiveVersion,
        InsertMap& insertMap,
        bool& done,
        Modifier& hashTable)
{
    // check whether we see this page for the first time in gc
    bool firstTime = (!mRecordCount);
    if(!mRecordCount)
        mRecordCount = *reinterpret_cast<uint32_t*>(mData);

    if (!needsCleaning(lowestActiveVersion, insertMap)) {
        // we are done - no cleaning needed for this page
        done = true;
        return mData;
    }

    // At this point we know that we will need to clean the page
    if(firstTime)
        markCurrentForDeletion();

    // construct fillpage if needed
    constructFillPage();

    // do a copyAndCompactPass over the data
    done = copyAndCompact(lowestActiveVersion, insertMap, hashTable);

    if (!done)
    {
        // if not done, this means we need to store the current fillpage and call gc again
        auto res = mFillPage;
        mFillPage = nullptr;
        return res;
    }

    // we are done. It might now be, that this page has some free space left
    if (insertMap.size())
        return fillWithInserts(lowestActiveVersion, insertMap, hashTable);  // this will return a non-nullptr
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

