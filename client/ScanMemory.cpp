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
#include <tellstore/ScanMemory.hpp>

#include <crossbow/infinio/InfinibandService.hpp>
#include <crossbow/logger.hpp>

namespace tell {
namespace store {

ScanMemoryManager::ScanMemoryManager(crossbow::infinio::InfinibandService& service, size_t chunkCount,
        size_t chunkLength)
        : mChunkLength(chunkLength),
          mRegion(service.allocateMemoryRegion(chunkCount * mChunkLength,
                IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE)),
          mChunkStack(chunkCount, nullptr) {
    auto data = mRegion.address();
    for (decltype(chunkCount) i = 0; i < chunkCount; ++i) {
        if (!mChunkStack.push(reinterpret_cast<void*>(data))) {
            throw std::runtime_error("Unable to push scan chunk on stack");
        }
        data += mChunkLength;
    }
}

ScanMemory ScanMemoryManager::acquire() {
    void* data;
    if (!mChunkStack.pop(data)) {
        return ScanMemory();
    }

    return ScanMemory(this, data, mChunkLength, mRegion.rkey());
}

void ScanMemoryManager::release(void* data) {
    LOG_ASSERT((reinterpret_cast<uintptr_t>(data) >= mRegion.address())
            && (reinterpret_cast<uintptr_t>(data) < mRegion.address() + mRegion.length()),
            "Data pointing not into the chunk")
    LOG_ASSERT((reinterpret_cast<uintptr_t>(data) - mRegion.address()) % mChunkLength == 0u,
            "Data pointing not at the beginning of a chunk")
    while (!mChunkStack.push(data));
}

ScanMemory::~ScanMemory() {
    if (mManager != nullptr) {
        mManager->release(mData);
    }
}

ScanMemory& ScanMemory::operator=(ScanMemory&& other) {
    if (mManager != nullptr) {
        mManager->release(mData);
    }
    mManager = other.mManager;
    other.mManager = nullptr;

    mData = other.mData;
    other.mData = nullptr;

    mLength = other.mLength;
    other.mLength = 0u;

    mKey = other.mKey;
    other.mKey = 0u;

    return *this;
}

} // namespace store
} // namespace tell
