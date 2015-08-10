#pragma once

#include <array>
#include <cstdint>
#include <limits>

namespace tell {
namespace store {

/**
 * @brief Class to store and test if a predicate evaluated to true
 */
class PredicateBitmap {
public:
    PredicateBitmap()
            : mMaxPosition(0x0u),
              mBitmap({}) {
    }

    inline bool test(uint8_t position);

    inline void set(uint8_t position);

    inline bool all();

private:
    using BlockType = uint64_t;

    static constexpr size_t BITS_PER_BLOCK = sizeof(BlockType) * 8u;

    static constexpr size_t CAPACITY = 4u;

    uint8_t mMaxPosition;
    std::array<BlockType, CAPACITY> mBitmap;
};

bool PredicateBitmap::test(uint8_t position) {
    if (position >= mMaxPosition) {
        mMaxPosition = position + 1;
        return false;
    }

    auto block = (position / BITS_PER_BLOCK);
    auto mask = (0x1u << (position % BITS_PER_BLOCK));
    return (mBitmap[block] & mask) != 0x0u;
}

void PredicateBitmap::set(uint8_t position) {
    if (position >= mMaxPosition) {
        mMaxPosition = position + 1;
    }

    auto block = (position / BITS_PER_BLOCK);
    auto mask = (0x1u << (position % BITS_PER_BLOCK));
    mBitmap[block] |= mask;
}

bool PredicateBitmap::all() {
    if (mMaxPosition == 0x0u) {
        return true;
    }

    auto lastBlock = ((mMaxPosition - 1) / BITS_PER_BLOCK);
    for (uint8_t block = 0x0u; block < lastBlock; ++block) {
        if (mBitmap[block] != std::numeric_limits<BlockType>::max()) {
            return false;
        }
    }

    auto mask = std::numeric_limits<BlockType>::max() >> (BITS_PER_BLOCK - (mMaxPosition % BITS_PER_BLOCK));
    return ((mBitmap[lastBlock] & mask) == mask);
}

} //namespace store
} //namespace tell
