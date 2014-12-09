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
} // namespace dmrewrite
} // namespace store
} // namespace tell
