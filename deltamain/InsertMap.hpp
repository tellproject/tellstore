#pragma once
#include <util/dense_hash_map.h>

#include <deque>

namespace tell {
namespace store {
namespace deltamain {

/**
 * This is a helper class for the
 * dense hash table we use to index
 * the entries in the insert log during
 * garbage collection.
 */
struct InsertMapKey {
    uint64_t key;
    constexpr InsertMapKey(uint64_t key) : key(key) {}
    constexpr InsertMapKey() : key(0) {}
    constexpr static InsertMapKey empty() {
        return InsertMapKey(std::numeric_limits<uint64_t>::max());
    }
    constexpr static InsertMapKey deleted() {
        return InsertMapKey(std::numeric_limits<uint64_t>::max() - 1);
    }
};

} // namespace deltamain
} // namespace store
} // namespace tell

namespace std {

template<>
struct equal_to<tell::store::deltamain::InsertMapKey> {
    bool operator() (
            tell::store::deltamain::InsertMapKey a,
            tell::store::deltamain::InsertMapKey b) const {
        return a.key == b.key;
    }
};

template<>
struct hash<tell::store::deltamain::InsertMapKey> {
    std::hash<uint64_t> mHash;
    size_t operator() (tell::store::deltamain::InsertMapKey k) const {
        return mHash(k.key);
    }
};

} // namspace std

namespace tell {
namespace store {
namespace deltamain {
using InsertMap = DenseHashMap<InsertMapKey, std::deque<const char*>>;
} // namespace deltamain
} // namespace store
} // namespace tell
