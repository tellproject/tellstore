#include "CuckooHash.hpp"

namespace tell {
namespace store {

CuckooTable::CuckooTable(PageManager& pageManager)
    : mPageManager(pageManager)
      , mPages(1, new(allocator::malloc(sizeof(PageWrapper))) PageWrapper(pageManager, pageManager.alloc()))
      , hash1(ENTRIES_PER_PAGE)
      , hash2(ENTRIES_PER_PAGE)
      , hash3(ENTRIES_PER_PAGE)
      , mSize(0) {
}

void* CuckooTable::get(uint64_t key) const {
    for (auto& hasher : {hash1, hash2, hash3}) {
        auto idx = hasher(key);
        const EntryT& entry = at(idx);
        if (entry.first == key) return entry.second;
    }
    return nullptr;
}

auto CuckooTable::at(size_t idx) const -> const EntryT& {
    auto tIdx = idx / ENTRIES_PER_PAGE;
    auto pIdx = idx - tIdx * ENTRIES_PER_PAGE;
    return (*mPages[tIdx])[pIdx];
}

Modifier CuckooTable::modifier(allocator& alloc) {
    return Modifier(*this, alloc);
}

CuckooTable::CuckooTable(PageManager& pageManager,
                         std::vector<CuckooTable::PageT>&& pages,
                         cuckoo_hash_function hash1,
                         cuckoo_hash_function hash2,
                         cuckoo_hash_function hash3,
                         size_t size)
    : mPageManager(pageManager), mPages(std::move(pages)), hash1(hash1), hash2(hash2), hash3(hash3), mSize(size) {
}

Modifier::~Modifier() {
    for (auto p : mToDelete) {
        alloc.free(p, [p]() {
            p->~PageWrapper();
        });
    }
}

CuckooTable* Modifier::done() const {
    return new(alloc.malloc(sizeof(CuckooTable))) CuckooTable(mTable.mPageManager, std::move(mPages), hash1, hash2,
                                                              hash3, mSize);
}

bool Modifier::insert(uint64_t key, void* value, bool replace /*= false*/) {
    while (true) {
        // we retry 20 times at the moment
        for (int i = 0; i < 20; ++i) {
            for (auto& h : {hash1, hash2, hash3}) {
                size_t pageIdx;
                auto idx = h(key);
                auto& entry = at(idx, pageIdx);
                auto e = &entry;
                if (cow(pageIdx)) {
                    e = &at(idx, pageIdx);
                }
                if (e->first == key) {
                    if (replace) e->second = value;
                    return true;
                } else if (e->second == nullptr) {
                    e->first = key;
                    e->second = value;
                    return false;
                } else {
                    std::pair<uint64_t, void*> p = *e;
                    e->first = key;
                    e->second = value;
                    key = p.first;
                    value = p.second;
                }
            }
        }
        rehash();
    }
}

bool Modifier::remove(uint64_t key) {
    size_t pIdx;
    bool res = false;
    for (auto& hash : {hash1, hash2, hash3}) {
        auto idx = hash(key);
        auto& entry = at(idx, pIdx);
        if (entry.first == key) {
            if (cow(pIdx)) {
                auto& e = at(idx, pIdx);
                res = e.second != nullptr;
                e.second = nullptr;
            } else {
                res = entry.second != nullptr;
                entry.second = nullptr;
            }
            --mSize;
            return res;
        }
    }
    return res;
}

Modifier::EntryT& Modifier::at(size_t idx, size_t& pageIdx) {
    pageIdx = idx / ENTRIES_PER_PAGE;
    auto pIdx = idx - pageIdx * ENTRIES_PER_PAGE;
    return (*mPages[pageIdx])[pIdx];
}

bool Modifier::cow(size_t idx) {
    if (pageWasModified[idx]) return false;
    pageWasModified[idx] = true;
    auto oldPage = mPages[idx];
    auto newPage = new(alloc.malloc(sizeof(PageWrapper))) PageWrapper(*oldPage);
    mPages[idx] = newPage;
    mToDelete.push_back(mPages[idx]);
    return true;
}

void Modifier::rehash() {
    auto capacity = mPages.size() * ENTRIES_PER_PAGE;
    if (5 * mSize / 4 > capacity || (mPages.size() > 1 && mSize / 5 > capacity)) {
        resize();
        return;
    }
    std::vector<PageT> oldPages = std::move(mPages);
    mPages = std::vector<PageT>(mPages.size(), nullptr);
    for (auto& e : mPages) {
        e = new PageWrapper(mTable.mPageManager, mTable.mPageManager.alloc());
    }
    hash1 = cuckoo_hash_function(capacity);
    hash2 = cuckoo_hash_function(capacity);
    hash3 = cuckoo_hash_function(capacity);
    for (size_t i = 0; i < oldPages.size(); ++i) {
        auto p = oldPages[i];
        auto& page = *p;
        for (size_t i = 0; i < ENTRIES_PER_PAGE; ++i) {
            if (page[i].second != nullptr)
                insert(page[i].first, page[i].second);
        }
        if (pageWasModified[i])
            mToDelete.push_back(p);
        else {
            alloc.free_now(p);
            pageWasModified[i];
        }
    }
}

void Modifier::resize() {
    auto capacity = mPages.size() * ENTRIES_PER_PAGE;
    auto numPages = mPages.size() * 2;
    if (mSize / 5 > capacity) {
        numPages /= 4;
    }
    if (numPages == 0) numPages = 1;
    std::vector<PageT> oldPages = std::move(mPages);
    mPages = std::vector<PageT>(numPages, nullptr);
    for (auto& e : mPages) {
        e = new PageWrapper(mTable.mPageManager, mTable.mPageManager.alloc());
    }
    hash1 = cuckoo_hash_function(capacity);
    hash2 = cuckoo_hash_function(capacity);
    hash3 = cuckoo_hash_function(capacity);
    for (size_t i = 0; i < oldPages.size(); ++i) {
        auto p = oldPages[i];
        auto& page = *p;
        for (size_t i = 0; i < ENTRIES_PER_PAGE; ++i) {
            if (page[i].second != nullptr)
                insert(page[i].first, page[i].second);
        }
        if (pageWasModified[i])
            mToDelete.push_back(p);
        else {
            alloc.free_now(p);
            pageWasModified[i];
        }
    }
}

} // namespace store
} // namespace tell
