#include "Page.hpp"
#include "InsertMap.hpp"

namespace tell {
namespace store {
namespace deltamain {

char* Page::gc(
        uint64_t lowestActiveVersion,
        InsertMap& insertMap,
        char*& fillPage,
        bool& done)
{
        // We iterate throgh our page
        auto size = usedMemory();
        uint64_t offset = mStartOffset;
        // in the first iteration we just decide wether we
        // need to collect any garbage here
        bool hasToClean = mStartOffset != 8;
        while (offset <= size && !hasToClean) {
            CDMRecord rec(mData + offset);
            if (rec.needsCleaning(lowestActiveVersion, insertMap)) {
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
                    insertMap,
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
        // we are done. It might now be, that this page has some free space left
        *reinterpret_cast<uint64_t*>(fillPage) = fillOffset;
        fillWithInserts(lowestActiveVersion, insertMap, fillPage);
        // now we write back the new size
        done = true;
        return res;
}

void Page::fillWithInserts(uint64_t lowestActiveVersion, InsertMap& insertMap, char*& fillPage)
{
    auto fillOffset = *reinterpret_cast<uint64_t*>(fillPage);
    char dummyRecord[DMRecord::spaceOverhead(RecordType::MULTI_VERSION_RECORD) + 4];
    dummyRecord[0] = to_underlying(RecordType::MULTI_VERSION_RECORD);
    // there are 0 number of versions
    *reinterpret_cast<uint32_t*>(dummyRecord + 4) = 0;
    *reinterpret_cast<const char**>(dummyRecord + 16) = nullptr;
    *reinterpret_cast<uint32_t*>(dummyRecord + 24) = 32;
    DMRecord dummy(dummyRecord);
    while (!insertMap.empty()) {
        bool couldRelocate;
        auto fst = insertMap.begin();
        uint64_t key = fst->first.key;
        dummy.writeKey(key);
        dummy.copyAndCompact(lowestActiveVersion,
                insertMap,
                fillPage + fillOffset,
                TELL_PAGE_SIZE - fillOffset,
                couldRelocate);
        if (couldRelocate) {
            insertMap.erase(fst);
        } else {
            break;
        }
    }
}

} // namespace deltamain
} // namespace store
} // namespace tell

