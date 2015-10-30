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
        mPages.emplace_back(reinterpret_cast<EntryT*>(pageManager.alloc()));
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
            if (!replace) {
                goto END;
            }
            break;
        }
        ++cnt;
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
                } else if (e->first == key) {
                    assert(replace);
                    if (cow(cnt, pageIdx)) {e = &at(cnt, idx, pageIdx);}
                    increment = false;
                    e->second = value;
                    res = true;
                    goto END;
                } else {
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
    bool res = false;
    unsigned cnt = 0;
    for (auto& hash : {hash1, hash2, hash3}) {
        auto idx = hash(key);
        auto& entry = at(cnt, idx, pIdx);
        if (entry.first == key) {
            if (cow(cnt, pIdx)) {
                auto& e = at(cnt, idx, pIdx);
                res = e.second != nullptr;
                e.second = nullptr;
            } else {
                res = entry.second != nullptr;
                entry.second = nullptr;
            }
            --mSize;
            goto END;
        }
        ++cnt;
    }
END:
    if (res) --mSize;
    return res;
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
    auto newPage = reinterpret_cast<EntryT*>(mTable.mPageManager.alloc());
    memcpy(newPage, oldPage, TELL_PAGE_SIZE);
    mToDelete.push_back(oldPage);
    mPages[3*idx + h] = newPage;
    return true;
}

void Modifier::rehash() {
    auto capacity = mPages.size()/3 * ENTRIES_PER_PAGE;
    if (5 * mSize / 4 > capacity || (mPages.size() > 1 && mSize / 5 > capacity)) {
        resize();
        return;
    }

    rehash(capacity, mPages.size());
}

void Modifier::resize() {
    auto capacity = mPages.size()/3 * ENTRIES_PER_PAGE;
    auto numPages = mPages.size();
    if (mSize / 5 > capacity) {
        numPages /= 2;
    } else {
        numPages *= 2;
    }
    assert(numPages % 3 == 0);
    capacity = numPages/3 * ENTRIES_PER_PAGE;
    if (numPages == 0) numPages = 1;
    assert(isPowerOf2(numPages/3 * ENTRIES_PER_PAGE));

    rehash(capacity, numPages);
}

void Modifier::rehash(size_t capacity, size_t numPages) {
    std::vector<EntryT*> oldPages;
    oldPages.swap(mPages);
    mPages.reserve(numPages);
    for (decltype(numPages) i = 0; i < numPages; ++i) {
        mPages.emplace_back(reinterpret_cast<EntryT*>(mTable.mPageManager.alloc()));
    }
    hash1 = cuckoo_hash_function(capacity);
    hash2 = cuckoo_hash_function(capacity);
    hash3 = cuckoo_hash_function(capacity);
    for (decltype(oldPages.size()) i = 0; i < oldPages.size(); ++i) {
        auto page = oldPages[i];
        for (size_t j = 0; j < ENTRIES_PER_PAGE; ++j) {
            if (page[j].second != nullptr)
                insert(page[j].first, page[j].second);
        }
        // If the page was modified it can be freed immediately (only the modifier had access to it)
        if (pageWasModified[i]) {
            mTable.mPageManager.free(page);
        } else {
            mToDelete.push_back(page);
            pageWasModified[i] = true;
        }
    }
    pageWasModified.resize(numPages, true);
}

size_t Modifier::capacity() const {
    return mPages.size() * ENTRIES_PER_PAGE;
}

size_t Modifier::size() const {
    return mSize;
}
} // namespace store
} // namespace tell
