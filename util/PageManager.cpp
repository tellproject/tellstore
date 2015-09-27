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
#include <sys/mman.h>
#include <cassert>
#include <memory.h>
#include <iostream>
#include <new>

#include <crossbow/logger.hpp>

namespace tell {
namespace store {

PageManager::PageManager(size_t size)
    : mData(mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, 0, 0)), mSize(size), mPages(
    size / TELL_PAGE_SIZE, nullptr)
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
        __attribute__((unused)) auto res = mPages.push(data);
        assert(res);
        data -= TELL_PAGE_SIZE;
    }
}

PageManager::~PageManager() {
    munmap(mData, mSize);
}

void* PageManager::alloc() {
    void* res = nullptr;
    __attribute__((unused)) auto success = mPages.pop(res);
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

const char *PageManager::getPageStart(const char *address) const {
    return reinterpret_cast<const char *> (
        reinterpret_cast<uint64_t>(address)
        - ((reinterpret_cast<uint64_t>(address) - reinterpret_cast<uint64_t>(mData)) % TELL_PAGE_SIZE)
        );
}

} // namespace store
} // namespace tell
