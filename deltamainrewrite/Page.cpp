#include "Page.hpp"

namespace tell {
namespace store {
namespace dmrewrite {


Page::Iterator Page::begin() {
    return Page::Iterator(mPage, 0);
}

Page::Iterator Page::end() {
    return Page::Iterator(mPage, TELL_PAGE_SIZE);
}

Page::Iterator Page::fromPosition(const Iterator& pos) {
    return Page::Iterator(mPage, pos.mOffset);
}
} // namespace dmrewrite
} // namespace store
} // namespace tell
