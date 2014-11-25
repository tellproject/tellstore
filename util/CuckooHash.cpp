#include "CuckooHash.hpp"

namespace tell {
namespace store {
namespace impl {

CuckooTable::CuckooTable(PageManager &pageManager)
    : mPageManager(pageManager),
      mPages(nullptr)
{
    auto v = new std::vector<PageT>(1, reinterpret_cast<PageT>(mPageManager.alloc()));
}

void *CuckooTable::get(uint64_t key) const {
    return nullptr;
}
} // namespace tell
} // namespace store
} // namespace impl
