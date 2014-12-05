#pragma once

#include <util/PageManager.hpp>

namespace tell {
namespace store {
namespace dmrewrite {

struct PageEntry {
    size_t offset;
};

/**
* This class is used to manage pages for the Delta Main
* Rewrite approach. A page has a very simple structure:
* The first 8 bytes are a pointer to the next page in
* the table (all pages form a simple linked list). The
* rest of the page is just a concatenation of all records
* stored in the page.
*/
class Page {
    PageManager& mPageManager;
    char* mPage;
public: // types
    class Iterator {
        const char* const mPage;
        size_t mOffset;
        Iterator(const char* const page, size_t offset)
            : mPage(page),
              mOffset(offset)
        {}
    public: // Types
        using difference_type = std::ptrdiff_t;
        using value_type = PageEntry;
        using iterator_category = std::forward_iterator_tag;
    public:
        Iterator& operator++ () {
            mOffset += *reinterpret_cast<const uint32_t*>(mPage + mOffset);
            return *this;
        }
        Iterator operator++ (int) {
            Iterator res(mPage, mOffset);
            return ++(*this);
        }
        bool operator== (const Iterator& other) const {
            return mOffset == other.mOffset;
        }
        bool operator!= (const Iterator& other) const {
            return mOffset != other.mOffset;
        }
        PageEntry operator*() const {
            return PageEntry{mOffset};
        }
    };
public:
    Page(PageManager& pageManager);
    Page(Page&&) = delete;
    Page(Page& other);
    ~Page();
    Page& operator= (Page& other);
    Page& operator= (Page&&) = delete;
public: // Access
};

} // namespace dmrewrite
} // namespace store
} // namespace tell
