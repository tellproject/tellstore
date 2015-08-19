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
