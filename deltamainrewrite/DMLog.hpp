#pragma once

#include <util/PageManager.hpp>

#include <cstddef>
#include <atomic>
#include <assert.h>

namespace tell {
namespace store {
namespace dmrewrite {

struct DMLogPage;

struct DMLogEntry {
private:
    std::atomic<uint32_t> mOffset;
public:
    uint32_t size;

    DMLogEntry(uint32_t offset, uint32_t size)
        : mOffset(offset + 1), size(size) {
    }

    char* data() {
        return reinterpret_cast<char*>(this) + sizeof(DMLogEntry);
    }

    const char* data() const {
        return reinterpret_cast<const char*>(this) + sizeof(DMLogEntry);
    }

    bool sealed() const {
        return mOffset.load() % 2 == 0;
    }

    uint32_t offset() const {
        auto offset = mOffset.load();
        return offset - (offset % 2);
    }

    void seal() {
        auto offset = mOffset.load();
        while (offset % 2 != 0) {
            mOffset.compare_exchange_strong(offset, offset - 1);
            offset = mOffset.load();
        }
    }

    std::pair<DMLogPage*, DMLogEntry*> nextP(DMLogPage* page);

    bool last() const;
    DMLogEntry* next();

    char* page() {
        return reinterpret_cast<char*>(this) - offset();
    }
};

/**
* A Log-Page has the following form:
*
* -------------------------------------------------------------------------------
* | next (8 bytes) | offset (4 bytes) | padding (4 bytes) | entry | entry | ... |
* -------------------------------------------------------------------------------
*
* This class, which only holds a pointer to the page, is mainly used
* for memory management.
*
* The last entry in the log is set to 0 (size and offset). We make
* use of the fact, that the PageManager only returns nulled pages.
*/
struct DMLogPage {
    static constexpr size_t LOG_HEADER_SIZE = 16;
    char* page;

    DMLogPage(char* page)
        : page(page) {
    }

    DMLogPage(const DMLogPage&) = delete;

    std::atomic<DMLogPage*>& next() {
        return *reinterpret_cast<std::atomic<DMLogPage*>*>(page);
    }

    std::atomic<uint32_t>& offset() {
        return *reinterpret_cast<std::atomic<uint32_t>*>(page + sizeof(DMLogPage*));
    }

    DMLogEntry* begin() {
        return reinterpret_cast<DMLogEntry*>(page);
    }

    constexpr static size_t DATA_OFFSET = 16;
};

class DMLog {
    constexpr static uint32_t MAX_SIZE = TELL_PAGE_SIZE - DMLogPage::DATA_OFFSET - sizeof(DMLogPage);
    PageManager& mPageManager;
    std::atomic<DMLogPage*> mHead;
    std::atomic<DMLogEntry*> mSealHead;
    std::pair<DMLogPage*, DMLogEntry*> mTail;
public:
    DMLog(PageManager& pageManager);

    DMLogEntry* append(uint32_t size);

    void seal(DMLogEntry* entry);

    DMLogEntry* tail();

    /**
    * Not thread safe
    */
    void setTail(DMLogEntry* nTail);
};

} // namespace dmrewrite
} // namespace store
} // namespace tell
