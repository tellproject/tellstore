#include "ColumnMapPage.hpp"
#include "deltamain/InsertMap.hpp"
#include "deltamain/Record.hpp"
#include "deltamain/Table.hpp"

#include <util/CuckooHash.hpp>

namespace tell {
namespace store {
namespace deltamain {

#include "ColumnMapUtils.in.cpp" // includes convenience functions for colum-layout

inline uint roundToMultiple(uint value, uint multiple) {
    return multiple*((value + (multiple-1))/multiple);
}

ColumnMapPage::ColumnMapPage(PageManager& pageManager, char* data, Table *table)
    : mPageManager(pageManager)
    , mTable(table)
    , mNullBitmapSize(getNullBitMapSize(table))
    , mNumFixedSized(table->getNumberOfFixedSizedFields())
    , mNumVarSized(table->getNumberOfVarSizedFields())
    , mNumColumns(mNumFixedSized + mNumVarSized)
    , mFixedValuesSize(table->getFieldOffset(table->getNumberOfFixedSizedFields()) - table->getFieldOffset(0))
    , mData(data)
    , mRecordCount(0)
    , mFillPageRecordCount(0)
    , mFillPageVarOffset(TELL_PAGE_SIZE)
    , mPageCleaningSummaries() {}

void ColumnMapPage::collectInserts(impl::VersionMap &versionMap,
                                   std::deque<const char*> &insertQueue,
                                   bool &newestIsDelete,
                                   bool &allVersionsInvalid) {
    for (auto queueIter = insertQueue.begin(); queueIter != insertQueue.end(); ++queueIter) {
        if (!newestIsDelete)
        {
            LOG_ERROR("if there is a newer insert, prior insert must point to a delete!");
            std::terminate();
        }
        CDMRecord rec (*queueIter);
        rec.collect(versionMap, newestIsDelete, allVersionsInvalid);
    }
}

void ColumnMapPage::pruneAndCount(uint64_t lowestActiveVersion,
                                  std::deque<RecordCleaningInfo> &pageCleaningSummary,
                                  RecordCleaningInfo &recInfo,
                                  impl::VersionMap &versionMap) {
    // prune no longer used versions of versionMap
    auto firstValidVersion = versionMap.lower_bound(lowestActiveVersion);
    if (firstValidVersion == versionMap.end()) {
        --firstValidVersion;
    }
    versionMap.erase(versionMap.begin(), firstValidVersion);

    // add information from versionMap to recordInfo
    auto highestVersionRecord = versionMap.rbegin();
    if (highestVersionRecord != versionMap.rend())
        recInfo.hasValidUpdatesOrInserts = (highestVersionRecord->second.type != RecordType::MULTI_VERSION_RECORD);
    for (auto iter = versionMap.begin(); iter != versionMap.end(); ++iter)
    {
        recInfo.tupleCount++;
        if (iter->second.type != RecordType::LOG_DELETE && iter->second.size != 0)  // size is 0 for deletes in MVRecord
            recInfo.totalVarSizedCount += roundToMultiple((iter->second.size - mFixedValuesSize), 4);
    }

    // prune recInfo if it is empty
    if (!recInfo.tupleCount)
        pageCleaningSummary.pop_back();
}

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
    mPageCleaningSummaries.emplace_back();
    mPageCleaningSummaries.back().first = mData;
    std::deque<RecordCleaningInfo> &pageCleaningSummary = mPageCleaningSummaries.back().second;

    keyVersionPtr = getKeyAt(0, mData);
    newestPtr = reinterpret_cast<char**>(const_cast<char*>(getNewestPtrAt(0, mData, mRecordCount)));
    varLengthPtr = getVarsizedLenghtAt(0, mData, mRecordCount, mNullBitmapSize);
    for (; keyVersionPtr < keyVersionPtrEnd; keyVersionPtr +=2, ++newestPtr, ++varLengthPtr) {
        pageCleaningSummary.emplace_back();
        RecordCleaningInfo &recInfo = pageCleaningSummary.back();
        recInfo.key = keyVersionPtr[0];
        auto &newest = recInfo.oldNewestPtr;
        auto &versionMap = recInfo.versionMap;
        newest = *newestPtr;
        bool safelyDiscard = (newest != nullptr);    // if safe is true, I can discard all versions below base version (if not, I have to keep the youngest)
        // iterate over all versions of this record and gather info
        for (; ; keyVersionPtr +=2, ++newestPtr, ++varLengthPtr) {
            if (!safelyDiscard || keyVersionPtr[1] >= lowestActiveVersion) {  //if version no longer valid, skip it
                if (*varLengthPtr == 0) {
                    // we have a delete record which means that we can delete the whole record
                    // iff everything else is below base version, otherwise we have to keep it
                    if (keyVersionPtr[0] == keyVersionPtr[2] && keyVersionPtr[3] >= lowestActiveVersion) {
                        // there are tuples above base version --> need to keep delete record and probe insertmap
                        versionMap.insert(std::make_pair(keyVersionPtr[1], impl::VersionHolder
                                {reinterpret_cast<char*>(const_cast<uint64_t*>(keyVersionPtr))+2,       // pointers for Col-MVRecords are semantically different from all others!
                                RecordType::MULTI_VERSION_RECORD,
                                0,
                                nullptr}));
                    }
                    safelyDiscard = true;
                }
                else if (*varLengthPtr > 0) {
                    versionMap.insert(std::make_pair(keyVersionPtr[1], impl::VersionHolder
                            {reinterpret_cast<char*>(const_cast<uint64_t*>(keyVersionPtr))+2,           // pointers for Col-MVRecords are semantically different from all others!
                            RecordType::MULTI_VERSION_RECORD,
                            (*varLengthPtr) + mFixedValuesSize,
                            nullptr}));
                    safelyDiscard = true;
                }
                // else: we have a revert entry which we can simply ignore
            }
            if (keyVersionPtr[0] != keyVersionPtr[2]) break;  //loop exit condition when there are no more records with the same key
        }

        // if this record has updates or inserts add them as needed and adjust newestptr and newestptr location
        bool newestIsDelete = true, allVersionsInvalid;
        if (newest)
        {
            CDMRecord rec (newest);
            rec.collect(versionMap, newestIsDelete, allVersionsInvalid);
            recInfo.oldNewestPtrLocation = reinterpret_cast<std::atomic<const char*>*>(newestPtr);
        }
        if (insertMap.count(keyVersionPtr[0])) {
            auto insertMapIter = insertMap.find(keyVersionPtr[0]);
            auto &queue = insertMapIter->second;
            collectInserts(versionMap, queue, newestIsDelete, allVersionsInvalid);
            insertMap.erase(insertMapIter); // insertionmap entry was consumed, delete it
        }

        // save newest pointer location
        for (auto iter = versionMap.begin(); iter != versionMap.end(); ++iter) {
            if (iter->second.type == RecordType::LOG_INSERT)
                recInfo.oldNewestPtrLocation = iter->second.newestPtrLocation;
        }

        // save newest pointer to the value that youngest insert pointed to
        // (which is equal to the address of the highest valid version or NULL if that's an insert)
        auto highestVersionRecord = versionMap.rbegin();
        if (highestVersionRecord != versionMap.rend())
            newest = (highestVersionRecord->second.type == RecordType::LOG_INSERT ?
                    nullptr :
                    const_cast<char *>(highestVersionRecord->second.record));

        // prune and count tuples and total consumption of var-sized values
        pruneAndCount(lowestActiveVersion, pageCleaningSummary, recInfo, versionMap);
    }

    return true;
}

void ColumnMapPage::copyLogRecord(uint64_t key,
                                  uint64_t version,
                                  impl::VersionHolder &logRecordVersionHolder,
                                  char *destBasePtr,
                                  uint32_t destIndex) {
    // copy key-version column
    auto keyVersionPtr = const_cast<uint64_t *>(getKeyAt(destIndex, destBasePtr));
    keyVersionPtr[0] = key;
    keyVersionPtr[1] = version;
    // newest ptr will be copied later
    // if this is a delete record, we are already done
    if (logRecordVersionHolder.type == RecordType::LOG_DELETE)
        return;
    char *srcPtr = const_cast<char *>(logRecordVersionHolder.record);
    // copy null bitmap
    memcpy(
        const_cast<char *>(getNullBitMapAt(destIndex, destBasePtr, mFillPageRecordCount, mNullBitmapSize)),
        srcPtr,
        mNullBitmapSize);
    srcPtr += mNullBitmapSize;
    // copy var-size column
    auto totalVarSize = logRecordVersionHolder.size - mFixedValuesSize;
    *(const_cast<int32_t *>(getVarsizedLenghtAt(destIndex, destBasePtr, mFillPageRecordCount, mNullBitmapSize)))
            = totalVarSize;

    // copy fixed-sized column data
    for (uint col = 0; col < mNumFixedSized; ++col) {
        auto fieldSize = mTable->getFieldSize((col));
        memcpy(
            getColumnNAt(mTable, col, destIndex, destBasePtr, mFillPageRecordCount, mNullBitmapSize),
            srcPtr,
            fieldSize);
        srcPtr += fieldSize;
    }

    // copy var heap
    memcpy(
        const_cast<char *>(destBasePtr + mFillPageVarOffset),
        srcPtr,
        totalVarSize);

    // copy var-sized column data
    for (uint col = mNumFixedSized; col < mNumColumns; ++col) {
        auto fieldSize = *(reinterpret_cast<uint32_t*>(srcPtr));    // field size including 4-byte size prefix
        auto destPtr = getColumnNAt(mTable, col, destIndex, destBasePtr, mFillPageRecordCount, mNullBitmapSize);
        *(reinterpret_cast<uint32_t*>(destPtr)) = mFillPageVarOffset;
        memcpy(
            &destPtr[4],
            &srcPtr[4],
            std::min(4u, (fieldSize-4)));     // copy 4-byte prefix (or less)
        mFillPageVarOffset += roundToMultiple(fieldSize, 4);
        srcPtr += roundToMultiple(fieldSize, 4);    // we know that var-values are also 4-byte aligned
    }
}

void ColumnMapPage::copyColumnRecords(char *srcBasePtr,
                                      uint32_t srcIndex,
                                      uint32_t srcRecordCount,
                                      uint32_t totalVarLenghtSize,
                                      char *destBasePtr,
                                      uint32_t destIndex,
                                      uint32_t numElements)
{
    // copy key-version column
    memcpy(const_cast<char *>(getKeyVersionPtrAt(destIndex, destBasePtr)), getKeyVersionPtrAt(srcIndex, srcBasePtr), 16*numElements);
    // newest ptrs will be copied later
    // copy null bitmpas
    mempcpy(
        const_cast<char *>(getNullBitMapAt(destIndex, destBasePtr, mFillPageRecordCount, mNullBitmapSize)),
        getNullBitMapAt(srcIndex, srcBasePtr, srcRecordCount, mNullBitmapSize),
        numElements * mNullBitmapSize);
    // var-size-length column
    memcpy(
        const_cast<int32_t *>(getVarsizedLenghtAt(destIndex, destBasePtr, mFillPageRecordCount, mNullBitmapSize)),
        getVarsizedLenghtAt(srcIndex, srcBasePtr, srcRecordCount, mNullBitmapSize),
        numElements * 4);

    // copy fixed-sized and var-sized column data
    for (uint col = 0; col < mNumColumns; ++col) {
        memcpy(
            getColumnNAt(mTable, col, destIndex, destBasePtr, mFillPageRecordCount, mNullBitmapSize),
            getColumnNAt(mTable, col, srcIndex, srcBasePtr, srcRecordCount, mNullBitmapSize),
            numElements * mTable->getFieldSize(col));
    }

    // copy var-sized heap data
    memcpy(
        const_cast<char *>(destBasePtr + mFillPageVarOffset),
        srcBasePtr + *(reinterpret_cast<uint32_t*>(getColumnNAt(mTable, mNumFixedSized, srcIndex, srcBasePtr, srcRecordCount, mNullBitmapSize))),
        totalVarLenghtSize);

    // adjust var-sized offsets
    uint32_t srcVarHeapOffset = *reinterpret_cast<uint32_t*>(getColumnNAt(mTable, 0, srcIndex, srcBasePtr, srcRecordCount, mNullBitmapSize));
    int32_t diff = int32_t(mFillPageVarOffset) - int32_t(srcVarHeapOffset);

    for (uint col = mNumFixedSized; col < mNumColumns; ++col) {
        auto destVarLenghtPtr = reinterpret_cast<uint32_t*>(getColumnNAt(mTable, col, destIndex, destBasePtr, mFillPageRecordCount, mNullBitmapSize));
        for (uint i = destIndex; i < numElements; ++i, destVarLenghtPtr += 2) {
            (*destVarLenghtPtr) = (int32_t(*destVarLenghtPtr) + diff);
        }
    }

    // adjust mFillPageVarOffset
    mFillPageVarOffset += totalVarLenghtSize;
}

//TODO: check whether this loop does the correct thing... doesn't
//this assume that the newestPtr is stored at the beginning of a
//log record (which is actually not the case)?
inline bool casNewest (std::atomic<const char*>* &ptrLocation, const char *expected, const char *desired) {
    auto ptr = reinterpret_cast<std::atomic<uint64_t>*>(ptrLocation);
    auto p = ptr->load();
    while (ptr->load() % 2) {
        ptr = reinterpret_cast<std::atomic<uint64_t>*>(p - 1);
        p = ptr->load();
    }
    uint64_t exp = reinterpret_cast<const uint64_t>(expected);
    uint64_t des = reinterpret_cast<const uint64_t>(desired);
    if (p != exp) return false;
    return ptr->compare_exchange_strong(exp, des);
}

char *ColumnMapPage::fillPage(Modifier& hashTable, RecordQueueIterator &end) {
    // construct fill page
    auto fillPage = reinterpret_cast<char*>(mPageManager.alloc());
    *(reinterpret_cast<uint32_t*>(fillPage)) = mFillPageRecordCount;

    // use this offset for constructing the new page
    mFillPageVarOffset = getSpaceConsumptionExeptHeap(mFillPageRecordCount, mNullBitmapSize, mFixedValuesSize);

    // copy data and adjust hash map (TODO: optimize copying of contiguous tuples accross record boundaries
    uint32_t destIndex = 0;
    for (auto pageSummaryIter = mPageCleaningSummaries.begin(); pageSummaryIter != mPageCleaningSummaries.end(); ++pageSummaryIter) {
        auto srcBasePtr = const_cast<char *>(pageSummaryIter->first);
        auto srcRecordCount = srcBasePtr ? *(reinterpret_cast<uint32_t*>(srcBasePtr)) : 0u;    // on "fill with inserts", baseptr is nullptr
        auto recordIterEnd = pageSummaryIter->second.end();
        for (auto recordIter = pageSummaryIter->second.begin(); recordIter != recordIterEnd && recordIter != end; ++recordIter) {
            recordIter->newNewestPtrLocation = reinterpret_cast<char**>(const_cast<char*>(getNewestPtrAt(destIndex, fillPage, mFillPageRecordCount)));
            auto key = recordIter->key;
            hashTable.insert(key, const_cast<char*>(getKeyVersionPtrAt(destIndex, fillPage))+2);
            auto versionIter = recordIter->versionMap.begin();
            auto versionIterEnd = recordIter->versionMap.end();
            while (versionIter != versionIterEnd) {
                if (versionIter->second.type == RecordType::MULTI_VERSION_RECORD) {
                    // copy ALL contiguous records
                    auto oldRecord = versionIter->second.record;
                    auto srcIndex = getIndex(srcBasePtr, oldRecord);
                    uint32_t numElements = 1;
                    uint32_t totalVarLenghtSize = roundToMultiple((versionIter->second.size - mFixedValuesSize), 4);
                    while ((++versionIter) != versionIterEnd) {
                        if (versionIter->second.type == RecordType::MULTI_VERSION_RECORD &&
                                (reinterpret_cast<const uint64_t>(versionIter->second.record) - reinterpret_cast<const uint64_t>(oldRecord)) != 16)
                            break;  // elements are not contiguous
                        numElements++;
                        totalVarLenghtSize += roundToMultiple((versionIter->second.size - mFixedValuesSize), 4);
                        oldRecord = versionIter->second.record;
                    }
                    copyColumnRecords(srcBasePtr, srcIndex, srcRecordCount, totalVarLenghtSize, fillPage, destIndex, numElements);
                    destIndex += numElements;
                }
                else {
                    // for log records, copy one at the time
                    copyLogRecord(key, versionIter->first, versionIter->second, fillPage, destIndex++);
                    ++versionIter;
                }
            }
        }
    }

    // adjust newest ptrs
    for (auto pageSummaryIter = mPageCleaningSummaries.begin(); pageSummaryIter != mPageCleaningSummaries.end(); ++pageSummaryIter) {
        auto recordIterEnd = pageSummaryIter->second.end();
        for (auto recordIter = pageSummaryIter->second.begin(); recordIter != recordIterEnd && recordIter != end; ++recordIter) {
            if (recordIter->oldNewestPtrLocation) {     // pure inserts (without prior values) we do not have to do this chasing
                auto expected = recordIter->oldNewestPtr;
                auto desired = reinterpret_cast<const char*>((recordIter->newNewestPtrLocation)) + 1;
                *(recordIter->newNewestPtrLocation) = expected;
                while(!(casNewest(recordIter->oldNewestPtrLocation, expected, desired)))
                    *(recordIter->newNewestPtrLocation) = (expected = const_cast<char*>(recordIter->oldNewestPtrLocation->load()));
            }
        }
    }

    // remove all page summaries and page summary entries that were consumed
    while (mPageCleaningSummaries.size() > 1)
        mPageCleaningSummaries.pop_front();
    auto summary = mPageCleaningSummaries.front().second;
    summary.erase(summary.begin(), end);

    //reset stats
    mFillPageRecordCount = 0;
    mFillPageVarOffset = TELL_PAGE_SIZE;
    return fillPage;
}

bool ColumnMapPage::checkRecordFits(RecordCleaningInfo &recInfo) {
    if (recInfo.totalVarSizedCount > mFillPageVarOffset)    // in that case surely no space left!
        return false;

    // new fillpage stats if we would apply record
    auto hypotheticalCount = mFillPageRecordCount + recInfo.tupleCount;
    auto hypotheticalVarOffset = mFillPageVarOffset - recInfo.totalVarSizedCount;

    // compute space requirements and check
    if (getSpaceConsumptionExeptHeap(hypotheticalCount, mNullBitmapSize, mFixedValuesSize) > hypotheticalVarOffset)
        return false;

    // otherwise we know that the record fits and will adjust the fillpage stats
    mFillPageRecordCount = hypotheticalCount;
    mFillPageVarOffset = hypotheticalVarOffset;
    return true;
}

char *ColumnMapPage::gcPass(Modifier& hashTable) {
    // mFillPageRecordCount and mFillPageVarOffsetPtr are set in accordance with all the
    // elements in mPageCleaningSummaries (except for the last entry about the current page) and
    // that all these records fit in a new page. We now have to analyze whether the entries of
    // the current page do as well
    auto &currentPageSummary = mPageCleaningSummaries.back().second;
    if (currentPageSummary.empty())
    {
        // the current page has no valid records at all -> mFillPage stats are the same
        // as before --> we are done with this page and should gc the next one
        mPageCleaningSummaries.pop_back();
        return nullptr;
    }

    for (auto iter = currentPageSummary.begin(); iter != currentPageSummary.end(); ++iter) {
        if (!checkRecordFits(*iter))
            return fillPage(hashTable, iter);
    }
    return nullptr;
}

char* ColumnMapPage::gc(uint64_t lowestActiveVersion, InsertMap& insertMap, bool& done, Modifier& hashTable) {
    // check whether we see this page for the first time in gc
    bool firstTime = (!mRecordCount);
    if(!mRecordCount)
        mRecordCount = *reinterpret_cast<uint32_t*>(mData);

    if (!needsCleaning(lowestActiveVersion, insertMap)) {
        // we are done - no cleaning needed for this page
        done = true;
        return mData;
    }

    // marke the page for deletion if we see it for the first time
    if(firstTime)
        markCurrentForDeletion();

    // do a gcPass over the data
    auto fillPage = gcPass(hashTable);
    done = (fillPage == nullptr);

    return fillPage;
}

char *ColumnMapPage::fillWithInserts(uint64_t lowestActiveVersion, InsertMap& insertMap, Modifier& hashTable) {
    // On the first call to fillWithInserts, we potentially have a set of page summaries and
    // want to generate a page summary for all inserts.
    // Subsequent calls will just consume from these summaries and the last call must clear the
    // insertmap (because this signals that filling has finished).

    if (mPageCleaningSummaries.front().first != nullptr) {
        // this is the first call, process insertMap
        mPageCleaningSummaries.emplace_back();
        mPageCleaningSummaries.back().first = nullptr;
        std::deque<RecordCleaningInfo> &pageCleaningSummary = mPageCleaningSummaries.back().second;
        for (auto insertMapIter = insertMap.begin(); insertMapIter != insertMap.end(); ++insertMapIter) {
            pageCleaningSummary.emplace_back();
            RecordCleaningInfo &recInfo = pageCleaningSummary.back();
            recInfo.key = insertMapIter->first.key;
            bool newestIsDelete = true, allVersionsInvalid;
            collectInserts(recInfo.versionMap, insertMapIter->second, newestIsDelete, allVersionsInvalid);
            pruneAndCount(lowestActiveVersion, pageCleaningSummary, recInfo, recInfo.versionMap);
        }
    }

    // fill page as soon as we have enough stuff to do so
    char *res = gcPass(hashTable);

    // If res is still nullptr, this means, that this is the last call and we can clear the insertMap in order to signal this.
    if (res == nullptr) {
        auto iter = mPageCleaningSummaries.back().second.end();
        res = fillPage(hashTable, iter);
    }

    return res;
}

} // namespace deltamain
} // namespace store
} // namespace tell

