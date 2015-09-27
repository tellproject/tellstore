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

#include <crossbow/fixed_size_stack.hpp>
#include <crossbow/infinio/InfinibandBuffer.hpp>
#include <crossbow/non_copyable.hpp>

#include <cstddef>
#include <cstdint>

namespace crossbow {
namespace infinio {
class InfinibandService;
} // namespace infinio
} // namespace crossbow

namespace tell {
namespace store {

class ScanMemory;

/**
 * @brief Buffer pool managing memory used for to store scan tuples
 */
class ScanMemoryManager : crossbow::non_copyable, crossbow::non_movable {
public:
    ScanMemoryManager(crossbow::infinio::InfinibandService& service, size_t chunkCount, size_t chunkLength);

    /**
     * @brief Acquire a buffer from the scan memory pool
     */
    ScanMemory acquire();

private:
    friend class ScanMemory;

    /**
     * @brief Release the data pointer to the chunk pool
     */
    void release(void* data);

    size_t mChunkLength;

    crossbow::infinio::AllocatedMemoryRegion mRegion;

    crossbow::fixed_size_stack<void*> mChunkStack;
};

/**
 * @brief Buffer to store scan tuples
 */
class ScanMemory : crossbow::non_copyable {
public:
    ScanMemory()
            : mManager(nullptr),
              mData(nullptr),
              mLength(0u),
              mKey(0u) {
    }

    ScanMemory(ScanMemoryManager* manager, void* data, size_t length, uint32_t key)
            : mManager(manager),
              mData(data),
              mLength(length),
              mKey(key) {
    }

    ~ScanMemory();

    ScanMemory(ScanMemory&& other)
            : mManager(other.mManager),
              mData(other.mData),
              mLength(other.mLength),
              mKey(other.mKey) {
        other.mManager = nullptr;
        other.mData = nullptr;
        other.mLength = 0u;
        other.mKey = 0u;
    }

    ScanMemory& operator=(ScanMemory&& other);

    bool valid() const {
        return (mManager != nullptr);
    }

    const void* data() const {
        return mData;
    }

    size_t length() const {
        return mLength;
    }

    uint32_t key() const {
        return mKey;
    }

private:
    ScanMemoryManager* mManager;

    void* mData;
    size_t mLength;
    uint32_t mKey;
};

} // namespace store
} // namespace tell
