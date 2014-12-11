#pragma once

#include <atomic>
#include <config.h>
#include <functional>
#include <random>
#include <memory.h>
#include "PageManager.hpp"
#include "Epoch.hpp"

namespace tell {
namespace store {

template<class T>
constexpr T log2Of(T x, T tmp = 0) {
    return x == 1 ? tmp : log2Of(x/2, tmp+1);
}

/**
* TODO: investigate into hash functions (this here is probably not optimal)
*
* This is a very simple hash function
* for cuckoo hashing.
*/
class cuckoo_hash_function {
    size_t mTableSize;
    uint64_t mSalt;
    uint64_t w_M; // m==64 w_M = w - M
public:
    cuckoo_hash_function(size_t tableSize)
        : mTableSize(tableSize), mSalt(0) {
        std::random_device rd;
        std::uniform_int_distribution<uint64_t> dist;
        while (mSalt == 0) {
            mSalt = dist(rd);
        }
        uint64_t M = log2Of(uint64_t(tableSize));
        w_M = 64 - M;
    }

    size_t operator()(uint64_t k) const {
        return (mSalt*k) >> (w_M);
    }
};

class Modifier;

constexpr bool isPowerOf2(size_t x) {
    return x == 1 || (x % 2  == 0 && isPowerOf2(x/2));
}

class PageWrapper {
public:
    static constexpr size_t ENTRIES_PER_PAGE = TELL_PAGE_SIZE / (sizeof(uint64_t) + sizeof(void*));
    static_assert(isPowerOf2(ENTRIES_PER_PAGE), "Entries per page needs to be a power of two");
    using EntryT = std::pair<uint64_t, void*>;
private:
    PageManager& mPageManager;
    EntryT* mPage;
public:
    explicit PageWrapper(PageManager& pageManager, void* page)
        : mPageManager(pageManager), mPage(reinterpret_cast<EntryT*>(page)) {
    }

    PageWrapper(const PageWrapper& other)
        : mPageManager(other.mPageManager), mPage(reinterpret_cast<EntryT*>(mPageManager.alloc())) {
        memcpy(mPage, other.mPage, TELL_PAGE_SIZE);
    }

    ~PageWrapper() {
        mPageManager.free(mPage);
    }

    const EntryT& operator[](size_t idx) const {
        return mPage[idx];
    }

    EntryT& operator[](size_t idx) {
        return mPage[idx];
    }
};

/**
* This is a simple hash table that uses
* Cuckoo hashing for efficient key lookup.
*
* This hash table allows for concurrent
* lookup, but writing has to be done single
* threaded. In this COW is used, to modify
* the table. As soon as the modification
* is done, a CAS operation is used to swap
* the new with the old version.
*/
class CuckooTable {
private:
    static constexpr size_t ENTRIES_PER_PAGE = PageWrapper::ENTRIES_PER_PAGE;
    using PageT = PageWrapper*;
    using EntryT = typename PageWrapper::EntryT;
    PageManager& mPageManager;
    std::vector<PageT> mPages;
    cuckoo_hash_function hash1;
    cuckoo_hash_function hash2;
    cuckoo_hash_function hash3;
    size_t mSize;
public:
    CuckooTable(PageManager& pageManager);

    friend class Modifier;

private:
    CuckooTable(PageManager& pageManager,
                std::vector<PageT>&& pages,
                cuckoo_hash_function hash1,
                cuckoo_hash_function hash2,
                cuckoo_hash_function hash3,
                size_t size);

public:
    void* get(uint64_t key) const;

    Modifier modifier(allocator& alloc);

    size_t capacity() const;

private: // helper functions
    const EntryT& at(unsigned h, size_t idx) const;
};

class Modifier {
    friend class CuckooTable;

    using PageT = typename CuckooTable::PageT;
    using EntryT = typename CuckooTable::EntryT;
    static constexpr size_t ENTRIES_PER_PAGE = CuckooTable::ENTRIES_PER_PAGE;
private:
    CuckooTable& mTable;
    allocator& alloc;
    std::vector<bool> pageWasModified;
    mutable std::vector<PageT> mPages;
    cuckoo_hash_function hash1;
    cuckoo_hash_function hash2;
    cuckoo_hash_function hash3;
    size_t mSize;
    std::vector<PageT> mToDelete;
private:
    Modifier(CuckooTable& table, allocator& alloc)
        : mTable(table),
          alloc(alloc),
          pageWasModified(table.mPages.size(), false),
          mPages(table.mPages),
          hash1(table.hash1),
          hash2(table.hash2),
          hash3(table.hash3),
          mSize(table.mSize)
    {
    }

public:
    ~Modifier();
    CuckooTable* done() const;

    /**
    * inserts or replaces a value. Will return true iff the key
    * did exist before in the hash table
    */
    bool insert(uint64_t key, void* value, bool replace = false);

    bool remove(uint64_t key);

    EntryT& at(unsigned h, size_t idx, size_t& pageIdx);

    size_t capacity() const;
    size_t size() const;

    bool cow(unsigned h, size_t idx);

    void rehash();

    void resize();
};

} // namespace store
} // namespace tell
