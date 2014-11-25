#include "PageManager.hpp"
#include <sys/mman.h>
#include <cassert>
#include <memory.h>

namespace tell {
namespace store {
namespace impl {

PageManager::PageManager(size_t size)
    : mData(mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, 0, 0)),
      mSize(size),
      mPages(size/PAGE_SIZE)
{
    assert(size % PAGE_SIZE == 0);
    auto numPages = size/PAGE_SIZE;
    memset(mData, 0, mSize);
    char* data = reinterpret_cast<char*>(mData);
    data += size - PAGE_SIZE; // data does now point to the last page
    for (size_t i = 0ul; i < numPages; ++i) {
        bool res = mPages.push(data);
        assert(res);
        data -= PAGE_SIZE;
    }
}

PageManager::~PageManager() {
    munmap(mData, mSize);
}

void *PageManager::alloc() {
    void* res = nullptr;
    bool success = mPages.pop(res);
    assert(success == (res != nullptr));
    return res;
}

void PageManager::free(void *page) {
    memset(page, 0, PAGE_SIZE);
    while (!mPages.push(page));
}
} // namespace tell
} // namespace store
} // namespace impl
