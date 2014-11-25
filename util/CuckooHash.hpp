#pragma once

#include <atomic>
#include <config.h>
#include <functional>
#include "PageManager.hpp"

namespace tell {
namespace store {
namespace impl {

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
    using PageT = std::pair<uint64_t, void*>*;
    PageManager& mPageManager;
    std::atomic<std::vector<PageT>*> mPages;
    std::hash<uint64_t> hasher;
public:
    class Modifier {
    private:
        CuckooTable& mTable;
    private:
        Modifier(CuckooTable& table)
                : mTable(table)
        {}
    };
public:
    CuckooTable(PageManager& pageManager);
    friend class Modifier;
public:
    void* get(uint64_t key) const;
};

} // namespace tell
} // namespace store
} // namespace impl
