#pragma once

#include <atomic>
#include <config.h>
#include <functional>
#include <random>
#include "PageManager.hpp"
#include "Epoch.hpp"

namespace tell {
namespace store {

/**
* TODO: investigate into hash functions (this here is probably not optimal)
*
* This is a very simple hash function
* for cuckoo hashing.
*/
class cuckoo_hash_function {
    size_t mTableSize;
    std::hash<uint64_t> hasher;
    uint64_t mSalt;
public:
    cuckoo_hash_function(size_t tableSize)
            : mTableSize(tableSize)
    {
        std::random_device rd;
        std::uniform_int_distribution<uint64_t> dist;
        mSalt = dist(rd);
    }

    size_t operator() (uint64_t k) const {
        return hasher(k + mSalt) & mTableSize;
    }
};

class Modifier;

class PageWrapper {
public:
    static constexpr size_t ENTRIES_PER_PAGE = TELL_PAGE_SIZE/(sizeof(uint64_t) + sizeof(void*));
    using EntryT = std::pair<uint64_t, void*>;
private:
    PageManager& mPageManager;
    EntryT* mPage;
public:
    explicit PageWrapper(PageManager& pageManager, void* page)
            : mPageManager(pageManager),
              mPage(reinterpret_cast<EntryT*>(page))
    {}
    PageWrapper(const PageWrapper& other)
            : mPageManager(other.mPageManager),
              mPage(reinterpret_cast<EntryT*>(mPageManager.alloc()))
    {
        memcpy(mPage, other.mPage, TELL_PAGE_SIZE);
    }
    ~PageWrapper() {
        mPageManager.free(mPage);
    }
    const EntryT& operator[] (size_t idx) const {
        return mPage[idx];
    }
    EntryT& operator[] (size_t idx) {
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
private: // helper functions
    const EntryT& at(size_t idx) const;
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
private:
    Modifier(CuckooTable& table, allocator& alloc)
            : mTable(table),
              alloc(alloc),
              pageWasModified(0, false),
              hash1(table.hash1),
              hash2(table.hash2),
              hash3(table.hash3)
    {}
    CuckooTable done() const;

    /**
    * inserts or replaces a value. Will return true iff the key
    * did exist before in the hash table
    */
    bool insert(uint64_t key, void* value, bool replace = false);
    bool remove(uint64_t key);
    EntryT& at(size_t idx, size_t& pageIdx);
    bool cow(size_t idx);
    void rehash();
    void resize();
};

} // namespace store
} // namespace tell
