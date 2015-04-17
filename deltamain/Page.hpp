#pragma once

#include <cstdint>
#include <cstddef>

#include <config.h>
#include <util/PageManager.hpp>
#include <util/Epoch.hpp>

#include "Record.hpp"

namespace tell {
namespace store {


class Page {
    /**
     * This class is used to
     * recycle pages in a save
     * manner.
     */
    class DummyPage {
    };
    PageManager& mPageManager;
    char* mData;
    uint64_t mStartOffset;
public:
    Page(PageManager& pageManager, char* data)
        : mPageManager(pageManager)
        , mData(data)
        , mStartOffset(8) {}


    uint64_t usedMemory(const char* data) const {
        return uint64_t(*reinterpret_cast<const uint64_t*>(data));
    }

    uint64_t usedMemory() const {
        return usedMemory(mData);
    }

    void markCurrentForDeletion() {
        auto oldPage = mData;
        auto& pageManager = mPageManager;
        allocator::free(new (allocator::malloc(sizeof(DummyPage))) DummyPage(),
                [oldPage, &pageManager]() { pageManager.free(oldPage); });
    }

    char* gc(uint64_t lowestActiveVersion, char*& fillPage, bool& done) {
        // We iterate throgh our page
        auto size = usedMemory();
        uint64_t offset = mStartOffset;
        // in the first iteration we just decide wether we
        // need to collect any garbage here
        bool hasToClean = mStartOffset != 8;
        while (offset < size && !hasToClean) {
            CDMRecord rec(mData + offset);
            if (rec.needsCleaning(lowestActiveVersion)) {
                hasToClean = true;
                break;
            }
            offset += rec.size();
        }
        if (!hasToClean) {
            // we are done - no cleaning needed for this page
            done = true;
            return mData;
        }
        // At this point we know that we will need to clean the page
        auto fillOffset = usedMemory(fillPage);
        char* res = fillOffset == 0 ? fillPage : nullptr;
        if (fillOffset == 0) fillOffset = 8;
        // now we also know that we will have to recycle the current
        // read only page
        markCurrentForDeletion();
        // now we need to iterate over the page again
        offset = mStartOffset;
        while (offset < size) {
            CDMRecord rec(mData + offset);
            bool couldRelocate = false;
            auto nSize = rec.copyAndCompact(lowestActiveVersion,
                    fillPage + fillOffset,
                    TELL_PAGE_SIZE - fillOffset,
                    couldRelocate);
            if (!couldRelocate) {
                // Before we do anything, we need to write back the new size
                // of the fillPage
                *reinterpret_cast<uint64_t*>(fillPage) = fillOffset;
                // The current fillPage is full
                // In this case we will either allocate a new fillPage (if
                // the old one got inserted before), or we will return to
                // indicate that we need a new fillPage
                if (res) {
                    done = false;
                    mStartOffset = offset;
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
            fillOffset += nSize;
            offset += rec.size();
        }
        // now we write back the new size
        *reinterpret_cast<uint64_t*>(fillPage) = fillOffset;
        done = true;
        return res;
    }
};

} // namespace store
} // namespace tell

