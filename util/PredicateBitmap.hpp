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
