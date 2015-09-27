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

#include <cstdint>
#include <cstddef>
#include <queue>

#include <config.h>
#include <util/PageManager.hpp>
#include <util/Scan.hpp>

#include "deltamain/Record.hpp"
#include "deltamain/InsertMap.hpp"

#include <crossbow/allocator.hpp>

namespace tell {
namespace store {

class CuckooTable;
class Modifier;

namespace deltamain {

/**
 * Contains the functionality for garbagabe collection and scan of a memory
 * page in column format. Please find the page layout description and the
 * single-record operation details in ColumnMapRecord.hpp.
 * In case of garbage collection, the page object should only be constructed
 * once for the first page and for all subsequent steps, the reset(.) method
 * should be called. This is important to keep state accross pages.
 */
class ColumnMapPage {
private:

    struct RecordCleaningInfo {
        char* oldNewestPtr = nullptr;                   // newest pointer value of old MV record
        std::atomic<const char*>* oldNewestPtrLocation = nullptr; // location of newest pointer to be CASed
        char** newNewestPtrLocation = nullptr;          // new location of newest pointer
        uint64_t key = 0;                               // record key
        uint32_t totalVarSizedCount = 0;                // bytes of var-heap consumption of this record
        uint16_t tupleCount = 0;                        // count of tuples that belong to this MV-record
        bool hasValidUpdatesOrInserts = false;          // flag to indicate that there are upates or inserts
        impl::VersionMap versionMap;                    // map of updates / inserts to this MV-record
    };

    PageManager& mPageManager;
    const Table *mTable;
    const size_t mNullBitmapSize;   //nullbitmap-size of table
    const uint32_t mNumFixedSized;  //number of fixed size columns of table
    const uint32_t mNumVarSized;    //number of var size columns of table
    const uint32_t mNumColumns;     //total number of columns of table
    const uint32_t mFixedValuesSize;//total byte size needed by all fixed values (needed to compute varSizedValuesSize from total size)
    char* mData;                    //current page
    uint32_t mRecordCount;          //record count of current page
    uint32_t mFillPageRecordCount;  //(hypothetical) record count of page to copy to
    uint32_t mFillPageVarOffset;    //(hypothetical) offset to var-heap of page to copy to
    std::deque<std::pair<const char*, std::deque<RecordCleaningInfo>>> mPageCleaningSummaries;
    // queue of meta-data about pages to be recycled, contains pairs of page-address and queues
    // where each queue stores one RecordCleaningInfo per record
    // invariant: after each call to gc, this summary either contains only one entry (namely of the current page) (-> done false)
    // or all summaries together occupy less than a page (-> done true)

private:

    /**
     * marks current page for deletion at the page manager
     */
    void markCurrentForDeletion() {
        auto oldPage = mData;
        auto& pageManager = mPageManager;
        crossbow::allocator::invoke([oldPage, &pageManager]() { pageManager.free(oldPage); });
    }

    /**
     * collects all inserts from a specific insert queue and stores them into versionMap
     */
    void collectInserts(impl::VersionMap &versionMap,
                        std::deque<const char *> &insertQueue,
                        bool &newestIsDelete,
                        bool &allVersionsInvalid);

    /**
     * prunes versions below baseVersion and does the tuple and size counting on recInfo
     */
    void pruneAndCount(uint64_t lowestActiveVersion,
                       std::deque<RecordCleaningInfo> &pageCleaningSummary,
                       RecordCleaningInfo &recInfo,
                       impl::VersionMap &versionMap);

    /**
     * checks whether cleaning is necessary for this page and as a side-product
     * adds an entry to mPageCleaningData if it does.
     */
    bool needsCleaning(uint64_t lowestActiveVersion, InsertMap& insertMap);

    /**
     * copies a log record into column format.
     * As a side effect, increases mFillPageVarOffset.
     */
    void copyLogRecord(uint64_t key,
                       uint64_t version,
                       impl::VersionHolder &logRecordVersionHolder,
                       char *destBasePtr,
                       uint32_t destIndex);

    /**
     * copies numElements subsequent column records from a source page to a
     * destination page. Assumes that destination page has enough space.
     * As a side effect, increases mFillPageVarOffset.
     */
    void copyColumnRecords(char *srcBasePtr,
                           uint32_t srcIndex,
                           uint32_t srcRecordCount,
                           uint32_t totalVarLenghtSize,
                           char *destBasePtr,
                           uint32_t destIndex,
                           uint32_t numElements);

    /**
     * creates a new fillpage by in-order-traversal of mPageCleaningSummary,
     * using the fillpage stats. Stops the traversal just before "end".
     */
    using RecordQueueIterator = std::_Deque_iterator<RecordCleaningInfo, RecordCleaningInfo&, RecordCleaningInfo*>;
    char *fillPage(Modifier& hashTable, RecordQueueIterator &end);

    /**
     * checks whether a new record would fit into the new fillpage and if so,
     * adjusts fillpage stats.
     */
    bool checkRecordFits(RecordCleaningInfo &recInfo);

    /**
     * performs a gc pass over consuming information from mPageCleaningSummary.
     * Returns nullptr if all data of this page was (hypothetically) consumed or the pointer
     * to the newly allocated page (that needs to be stored) if we are not done yet.
     */
    char *gcPass(Modifier& hashTable);

public:

    ColumnMapPage(PageManager& pageManager, char* data, Table *table);

    void reset(char *data)
    {
        mData = data;
        mRecordCount = 0;   // set it to 0 in order to know that we see this page the first time (which is important for recycling it)
    }

    /**
     * Performs a gc step on this page and copies data to a new page.
     * This call either ends when there are
     * (a) no changes to be done (done is set to true and result is set to current page) --> store page in page list, call reset() with address of next page, followed by gc ()
     * (b) no records to copy anymore (done is set to true and result is set to nullptr otherwise) --> call reset() with the address of the next page, followed by gc()
     * (c) new page is full (done is set to false and result is set to new page) --> store new page in page list and call gc() again
     */
    char *gc(uint64_t lowestActiveVersion, InsertMap& insertMap, bool& done, Modifier& hashTable);

    /**
     * fills inserts (from the insertmap) into a new page.
     * Returns the address of that page such that it can be stored.
     */
    char *fillWithInserts(uint64_t lowestActiveVersion, InsertMap& insertMap, Modifier& hashTable);
};

} // namespace deltamain
} // namespace store
} // namespace tell
