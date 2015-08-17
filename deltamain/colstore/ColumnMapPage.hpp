#pragma once

#include <cstdint>
#include <cstddef>

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
    PageManager& mPageManager;
    Table *mTable;
    char* mData;                    //current page
    uint32_t mRecordCount;          //record count of current page
    uint32_t mStartIndex;           //index of the first key that needs to be inspected in gc
    char* mFillPage;                //page to copy to
    char* mFillPageVarOffsetPtr;    //offset to var-heap of page to copy to
    uint32_t mFillPageRecordCount;  //record count of page to copy to

private:

    /**
     * construct new fill page if needed
     */
    void constructFillPage()
    {
        if (!mFillPage)
        {
            mFillPage = reinterpret_cast<char*>(mPageManager.alloc());
            mFillPageVarOffsetPtr = mFillPage + TELL_PAGE_SIZE;
            mFillPageRecordCount = 0;
        }
    }


    void markCurrentForDeletion() {
        auto oldPage = mData;
        auto& pageManager = mPageManager;
        crossbow::allocator::invoke([oldPage, &pageManager]() { pageManager.free(oldPage); });
    }

public:

    ColumnMapPage(PageManager& pageManager, char* data, Table *table)
        : mPageManager(pageManager)
        , mTable(table)
        , mData(data)
        , mRecordCount(*(reinterpret_cast<uint32_t*>(mData)))
        , mStartIndex(0)
        , mFillPage(nullptr)
        , mFillPageVarOffsetPtr(nullptr)
        , mFillPageRecordCount(0) {}

    void reset(char *data)
    {
        mData = data;
        mRecordCount = *(reinterpret_cast<uint32_t*>(mData));
        mStartIndex = 0;
    }

    /**
     * Performs a gc step on this page and copies data to a new page.
     * This call either ends when there are
     * (a) no changes to be done (done is set to true and result is set to current page) --> store page in page list, call reset() with address of next page, followed by gc ()
     * (b) no records to copy anymore (done is set to true and result is set to newpage if this is one is full as well or nullptr otherwise) --> call reset() with the address of the next page, followed by gc()
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
