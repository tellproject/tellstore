#pragma once

#include <config.h>
#include <util/PageManager.hpp>

namespace tell {
namespace store {
namespace dmrewrite {

struct PageEntry {
    size_t offset;
};

/**
* Helper class to manipulate pages
*/
class Page {
    char* mPage;
public: // types
    class Iterator {
        friend class Page;
        const char* mPage;
        size_t mOffset;
        Iterator(const char* const page, size_t offset)
            : mPage(page),
              mOffset(offset)
        {
            if (*reinterpret_cast<const uint32_t*>(mPage + mOffset) == 0)
                mOffset = TELL_PAGE_SIZE;
        }
    public: // Types
        using difference_type = std::ptrdiff_t;
        using value_type = PageEntry;
        using iterator_category = std::forward_iterator_tag;
    public:
        Iterator& operator++ () {
            mOffset += *reinterpret_cast<const uint32_t*>(mPage + mOffset) + 8;
            if (*reinterpret_cast<const uint32_t*>(mPage + mOffset) == 0)
                mOffset = TELL_PAGE_SIZE;
            return *this;
        }
        Iterator operator++ (int) {
            Iterator res(mPage, mOffset);
            return ++(*this);
        }
        bool operator== (const Iterator& other) const {
            if (mPage != other.mPage) return false;
            return mOffset == other.mOffset;
        }
        bool operator!= (const Iterator& other) const {
            if (mPage != other.mPage) return true;
            return mOffset != other.mOffset;
        }
        size_t operator*() const {
            return mOffset;
        }
    };
public:
    Page(char* p) : mPage(p) {}
public: // Access
    Iterator begin();
    Iterator end();
    Iterator fromPosition(const Iterator& pos);
    char* getRecord(const Iterator& i) {
        return (mPage + *i);
    }
};

} // namespace dmrewrite
} // namespace store
} // namespace tell
