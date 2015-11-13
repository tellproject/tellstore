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

#include "CuckooHash.hpp"

#include <crossbow/logger.hpp>

namespace tell {
namespace store {

CuckooTable::CuckooTable(PageManager& pageManager)
    : mPageManager(pageManager)
      , hash1(ENTRIES_PER_PAGE)
      , hash2(ENTRIES_PER_PAGE)
      , hash3(ENTRIES_PER_PAGE)
      , mSize(0)
{
    mPages.reserve(3);
    for (size_t i = 0; i < 3; ++i) {
        auto page = pageManager.alloc();
        if (!page) {
            LOG_ERROR("PageManager ran out of space");
            std::terminate();
        }
        mPages.emplace_back(reinterpret_cast<EntryT*>(page));
    }
}

CuckooTable::~CuckooTable() {
}

void CuckooTable::destroy() {
    for (auto p : mPages) {
        mPageManager.free(p);
    }
}

const void* CuckooTable::get(uint64_t key) const {
    unsigned cnt = 0;
    for (auto& hasher : {hash1, hash2, hash3}) {
        auto idx = hasher(key);
        const EntryT& entry = at(cnt, idx);
        if (entry.first == key) return entry.second;
        ++cnt;
    }
    return nullptr;
}

auto CuckooTable::at(unsigned h, size_t idx) const -> const EntryT& {
    auto tIdx = idx / ENTRIES_PER_PAGE;
    auto pIdx = idx - tIdx * ENTRIES_PER_PAGE;
    return mPages[3*tIdx+h][pIdx];
}

Modifier CuckooTable::modifier() {
    return Modifier(*this);
}

CuckooTable::CuckooTable(PageManager& pageManager,
                         std::vector<EntryT*>&& pages,
                         cuckoo_hash_function hash1,
                         cuckoo_hash_function hash2,
                         cuckoo_hash_function hash3,
                         size_t size)
    : mPageManager(pageManager), mPages(std::move(pages)), hash1(hash1), hash2(hash2), hash3(hash3), mSize(size) {
}

size_t CuckooTable::capacity() const {
    return ENTRIES_PER_PAGE * mPages.size();
}

Modifier::~Modifier() {
    if (mToDelete.empty()) {
        return;
    }

    auto pages = std::move(mToDelete);
    auto& pageManager = mTable.mPageManager;
    crossbow::allocator::invoke([pages, &pageManager]() {
        for (auto page : pages) {
            pageManager.free(page);
        }
    });
}

CuckooTable* Modifier::done() const {
    return crossbow::allocator::construct<CuckooTable>(mTable.mPageManager, std::move(mPages), hash1, hash2, hash3,
            mSize);
}

const void* Modifier::get(uint64_t key) const
{
    unsigned cnt = 0;
    for (auto& h : {hash1, hash2, hash3}) {
        size_t pageIdx;
        auto idx = h(key);
        auto& entry = at(cnt, idx, pageIdx);
        if (entry.first == key) {
            return entry.second;
            break;
        }
        ++cnt;
    }
    return nullptr;
}

bool Modifier::insert(uint64_t key, void* value, bool replace /*= false*/) {
    LOG_ASSERT(value != nullptr, "Value must not be null");

    // we first check, whether the value exists
    bool res = false;
    bool increment = true;
    unsigned cnt = 0;
    for (auto& h : {hash1, hash2, hash3}) {
        size_t pageIdx;
        auto idx = h(key);
        auto& entry = at(cnt, idx, pageIdx);
        if (entry.first == key && entry.second != nullptr) {
            if (replace) {
                auto e = &entry;
                if (cow(cnt, pageIdx)) {
                    e = &at(cnt, idx, pageIdx);
                }
                e->second = value;
                increment = false;
                res = true;
            }
            goto END;
        }
        ++cnt;
    }
    if (replace) {
        goto END;
    }
    // actual insert comes here
    while (true) {
        // we retry 20 times at the moment
        for (int i = 0; i < 20; ++i) {
            cnt = 0;
            for (auto& h : {hash1, hash2, hash3}) {
                size_t pageIdx;
                auto idx = h(key);
                auto& entry = at(cnt, idx, pageIdx);
                auto e = &entry;
                if (e->second == nullptr) {
                    if (cow(cnt, pageIdx)) {
                        e = &at(cnt, idx, pageIdx);
                    }
                    e->first = key;
                    e->second = value;
                    res = true;
                    goto END;
                } else {
                    assert(e->first != key);
                    if (cow(cnt, pageIdx)) {
                        e = &at(cnt, idx, pageIdx);
                    }
                    std::pair<uint64_t, void*> p = *e;
                    e->first = key;
                    e->second = value;
                    key = p.first;
                    value = p.second;
                }
                ++cnt;
            }
        }
        rehash();
    }
END:
    if (res && increment)
        ++mSize;
    return res;
}

bool Modifier::remove(uint64_t key) {
    size_t pIdx;
    unsigned cnt = 0;
    for (auto& hash : {hash1, hash2, hash3}) {
        auto idx = hash(key);
        auto& entry = at(cnt, idx, pIdx);
        auto e = &entry;
        if (e->first == key && e->second != nullptr) {
            if (cow(cnt, pIdx)) {
                e = &at(cnt, idx, pIdx);
            }
            e->second = nullptr;
            --mSize;
            return true;
        }
        ++cnt;
    }
    return false;
}

Modifier::EntryT& Modifier::at(unsigned h, size_t idx, size_t& pageIdx) {
    pageIdx = idx / ENTRIES_PER_PAGE;
    auto pIdx = idx - pageIdx * ENTRIES_PER_PAGE;
    return mPages[3*pageIdx + h][pIdx];
}

auto Modifier::at(unsigned h, size_t idx, size_t& pageIdx) const -> const EntryT&
{
    return const_cast<Modifier*>(this)->at(h, idx, pageIdx);
}

bool Modifier::cow(unsigned h, size_t idx) {
    if (pageWasModified[3*idx + h]) return false;
    pageWasModified[3*idx + h] = true;
    auto oldPage = mPages[3*idx + h];
    auto page = mTable.mPageManager.alloc();
    if (!page) {
        LOG_ERROR("PageManager ran out of space");
        std::terminate();
    }
    auto newPage = reinterpret_cast<EntryT*>(page);
    memcpy(newPage, oldPage, TELL_PAGE_SIZE);
    mToDelete.push_back(oldPage);
    mPages[3*idx + h] = newPage;
    return true;
}

void Modifier::rehash() {
    auto totalCapacity = mPages.size() * ENTRIES_PER_PAGE;
    auto numPages = mPages.size();
    // Double number of pages if usage is above 80 percent, halve if below 20 percent
    if (4 * mSize / 5 > totalCapacity) {
        numPages *= 2;
    } else if (numPages > 3 && mSize / 5 < totalCapacity) {
        numPages /= 2;
    }
    LOG_ASSERT(numPages >= 3, "Need at least 3 pages");
    LOG_ASSERT(numPages % 3 == 0, "Number of pages must be divisible by 3");

    // Allocate pages
    std::vector<EntryT*> oldPages;
    oldPages.swap(mPages);
    mPages.reserve(numPages);
    for (decltype(numPages) i = 0; i < numPages; ++i) {
        auto page = mTable.mPageManager.alloc();
        if (!page) {
            LOG_ERROR("PageManager ran out of space");
            std::terminate();
        }
        mPages.emplace_back(reinterpret_cast<EntryT*>(page));
    }

    std::vector<bool> oldPageWasModified(numPages, true);
    oldPageWasModified.swap(pageWasModified);

    // Allocate hash functions
    auto hashCapacity = (numPages / 3) * ENTRIES_PER_PAGE;
    LOG_ASSERT(isPowerOf2(hashCapacity), "Hash capacity must be power of 2");
    hash1 = cuckoo_hash_function(hashCapacity);
    hash2 = cuckoo_hash_function(hashCapacity);
    hash3 = cuckoo_hash_function(hashCapacity);

    // Copy elements from old pages into new pages
    for (decltype(oldPages.size()) i = 0; i < oldPages.size(); ++i) {
        auto page = oldPages[i];
        for (size_t j = 0; j < ENTRIES_PER_PAGE; ++j) {
            if (page[j].second != nullptr)
                insert(page[j].first, page[j].second);
        }
        // If the page was modified it can be freed immediately (only the modifier had access to it)
        if (oldPageWasModified[i]) {
            mTable.mPageManager.free(page);
        } else {
            mToDelete.push_back(page);
        }
    }
}

size_t Modifier::capacity() const {
    return mPages.size() * ENTRIES_PER_PAGE;
}

size_t Modifier::size() const {
    return mSize;
}
} // namespace store
} // namespace tell
