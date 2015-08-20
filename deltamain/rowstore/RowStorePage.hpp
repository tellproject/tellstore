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
 * page in row format (with RowStorePage::Iterator).
 * In case of garbage collection, the page object should only be constructed
 * once for the first page and for all subsequent steps, the reset(.) method
 * should be called. This is important to keep state accross pages.
 */
class RowStorePage {
    PageManager& mPageManager;
    char* mData;
    uint64_t mSize;             //size (used memory) of current page (first 8 bits of data)
    uint64_t mStartOffset;      //offset in current page where gc proceeds
    char* mFillPage;            //page to copy to
    uint64_t mFillOffset;       //offset in fillpage to copy to next

private:

    /**
     * construct new fill page if needed
     */
    void constructFillPage()
    {
        if (!mFillPage)
        {
            mFillPage = reinterpret_cast<char*>(mPageManager.alloc());
            mFillOffset = 8;
        }
    }


    void markCurrentForDeletion() {
        auto oldPage = mData;
        auto& pageManager = mPageManager;
        crossbow::allocator::invoke([oldPage, &pageManager]() { pageManager.free(oldPage); });
    }

public:
    class Iterator {
        friend class RowStorePage;
    private:
        const char* current;
        Iterator(const char* current) : current(current) {}
    public:
        Iterator() {}
        Iterator(const Iterator&) = default;
        Iterator& operator=(const Iterator&) = default;
    public:
        Iterator& operator++();
        Iterator operator++(int);
        bool operator==(const Iterator& other) const;
        bool operator!=(const Iterator& other) const {
            return !(*this == other);
        }
        const char* operator*() const;
    };

    RowStorePage(PageManager& pageManager, char* data, Table *table = nullptr)
        : mPageManager(pageManager)
        , mData(data)
        , mSize(uint64_t(*reinterpret_cast<const uint64_t*>(data)))
        , mStartOffset(8)
        , mFillPage(nullptr)
        , mFillOffset(0) {}

    void reset(char *data)
    {
        mData = data;
        mStartOffset = 8;
        mSize = uint64_t(*reinterpret_cast<const uint64_t*>(data));
    }

    /**
     * Performs a gc step on this page and copies data to a new page.
     * This call either ends when there are
     * (a) no changes to be done (done is set to true and result is set to current page) --> store page in page list, call reset() with address of next page, followed by gc ()
     * (b) no records to copy anymore (done is set to true and result is set to nullptr otherwise) --> call reset() with the address of the next page, followed by gc()
     * (c) new page is full (done is set to false and result is set to new page) --> store new page in page list and call gc() again
     */
    char* gc(uint64_t lowestActiveVersion, InsertMap& insertMap, bool& done, Modifier& hashTable);

    /**
     * fills inserts (from the insertmap) into a new page.
     * Returns the address of that page such that it can be stored.
     */
    char *fillWithInserts(uint64_t lowestActiveVersion, InsertMap& insertMap, Modifier& hashTable);

    Iterator begin() const;
    Iterator end() const;
};

} // namespace deltamain
} // namespace store
} // namespace tell
