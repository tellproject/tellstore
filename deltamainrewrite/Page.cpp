#include "Page.hpp"

namespace tell {
namespace store {
namespace dmrewrite {

Page::Page(PageManager& pageManager)
    : mPageManager(pageManager),
      mPage(reinterpret_cast<char*>(pageManager.alloc()))
{
}

Page::~Page() {
    mPageManager.free(mPage);
}

Page::Page(Page& other)
    : mPageManager(other.mPageManager),
      mPage(reinterpret_cast<char*>(mPageManager.alloc()))
{
    memcpy(mPage, other.mPage, TELL_PAGE_SIZE);
}

Page& Page::operator=(Page& other) {
    memcpy(mPage, other.mPage, TELL_PAGE_SIZE);
    return *this;
}
} // namespace dmrewrite
} // namespace store
} // namespace tell
