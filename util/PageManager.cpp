#include "PageManager.hpp"
#include <sys/mman.h>
#include <cassert>
#include <memory.h>
#include <iostream>
#include <new>

#include "Logging.hpp"

namespace tell {
namespace store {

PageManager::PageManager(size_t size)
    : mData(mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, 0, 0)), mSize(size), mPages(
    size / TELL_PAGE_SIZE)
{
    if (mData == MAP_FAILED) {
        throw std::bad_alloc();
    }
    assert(size % TELL_PAGE_SIZE == 0);
    auto numPages = size / TELL_PAGE_SIZE;
    memset(mData, 0, mSize);
    char* data = reinterpret_cast<char*>(mData);
    data += size - TELL_PAGE_SIZE; // data does now point to the last page
    for (size_t i = 0ul; i < numPages; ++i) {
        bool res = mPages.push(data);
        assert(res);
        data -= TELL_PAGE_SIZE;
    }
}

PageManager::~PageManager() {
    munmap(mData, mSize);
}

void* PageManager::alloc() {
    void* res = nullptr;
    bool success = mPages.pop(res);
    assert(success == (res != nullptr));
    assert(res >= mData && res < reinterpret_cast<char*>(mData) + mSize);
    assert((reinterpret_cast<char*>(res) - reinterpret_cast<char*>(mData)) % TELL_PAGE_SIZE == 0);
    return res;
}

void PageManager::free(void* page) {
    assert(page >= mData && page < reinterpret_cast<char*>(mData) + mSize);
    assert((reinterpret_cast<char*>(page) - reinterpret_cast<char*>(mData)) % TELL_PAGE_SIZE == 0);
    memset(page, 0, TELL_PAGE_SIZE);
    freeEmpty(page);
}

void PageManager::freeEmpty(void* page) {
    while (!mPages.push(page));
}

} // namespace store
} // namespace tell
