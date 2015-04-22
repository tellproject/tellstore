#pragma once
#include <config.h>

#include "PageManager.hpp"

#include <cstring>
#include <random>

namespace tell {
namespace store {
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
* This is a very simple general hash function.
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

} // namespace store
} // namespace tell
