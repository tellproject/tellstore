/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */

#include "PageManager.hpp"

#include <crossbow/logger.hpp>

#include <iostream>
#include <new>

#include <sys/mman.h>
#include <memory.h>

namespace tell {
namespace store {

PageManager::PageManager(size_t size)
    : mData(mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, 0, 0)),
      mSize(size),
      mPages(size / TELL_PAGE_SIZE, nullptr)
{
    if (mData == MAP_FAILED) {
        throw std::bad_alloc();
    }
    LOG_ASSERT(mSize % TELL_PAGE_SIZE == 0, "Size must divide the page size");
    auto numPages = mSize / TELL_PAGE_SIZE;
    memset(mData, 0, mSize);
    auto data = reinterpret_cast<char*>(mData);
    data += mSize - TELL_PAGE_SIZE; // data does now point to the last page
    for (decltype(numPages) i = 0ul; i < numPages; ++i) {
        __attribute__((unused)) auto res = mPages.push(data);
        LOG_ASSERT(res, "Pusing page did not succeed");
        data -= TELL_PAGE_SIZE;
    }
    LOG_ASSERT(mPages.size() == numPages, "Not all pages were added to the stack");
}

PageManager::~PageManager() {
    LOG_ASSERT(mPages.capacity() == mPages.size(), "Not all pages released");
    munmap(mData, mSize);
}

void* PageManager::alloc() {
    void* page;
    auto success = mPages.pop(page);
    LOG_ASSERT(!success || (page != nullptr), "Successful pop must not return null pages");
    LOG_ASSERT(!success || (page >= mData && page < reinterpret_cast<char*>(mData) + mSize), "Page points out of bound");
    LOG_ASSERT(!success || (reinterpret_cast<char*>(page) - reinterpret_cast<char*>(mData)) % TELL_PAGE_SIZE == 0,
            "Pointer points not to beginning of page");
    if (!success) {
        return nullptr;
    }
    memset(page, 0, TELL_PAGE_SIZE);
    return page;
}

void PageManager::free(void* page) {
    LOG_ASSERT(page != nullptr, "Page must not be null");
    LOG_ASSERT(page >= mData && page < reinterpret_cast<char*>(mData) + mSize, "Page points out of bound");
    LOG_ASSERT((reinterpret_cast<char*>(page) - reinterpret_cast<char*>(mData)) % TELL_PAGE_SIZE == 0,
            "Pointer points not to beginning of page");
    freeEmpty(page);
}

void PageManager::free(const std::vector<void*>& pages) {
    for (auto page : pages) {
        free(page);
    }
}

void PageManager::freeEmpty(void* page) {
    while (!mPages.push(page));
}

} // namespace store
} // namespace tell
