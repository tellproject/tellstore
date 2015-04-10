#include "CommitManager.hpp"
#include <algorithm>
#include <cstring>
#include <memory>

namespace tell {
namespace store {

DummyManager::DummyManager()
    : mVersions(new unsigned char[INITAL_BUFFER_SIZE]),
      mVersionsLength(INITAL_BUFFER_SIZE),
      mBase(0u),
      mLastVersion(0u),
      mLowestActiveVersion(0u),
      mActiveBaseVersions(new uint32_t[INITAL_BUFFER_SIZE]),
      mActiveBaseVersionsCount(INITAL_BUFFER_SIZE)
#ifndef NDEBUG
      , mRunningTransactions(0)
#endif
{
    memset(mVersions, 0, INITAL_BUFFER_SIZE);
    memset(mActiveBaseVersions, 0, sizeof(uint32_t)*INITAL_BUFFER_SIZE);
}

DummyManager::~DummyManager() {
    delete[] mActiveBaseVersions;
    delete[] mVersions;
}

SnapshotDescriptor DummyManager::startTx() {
    Lock _(mMutex);
#ifndef NDEBUG
    ++mRunningTransactions;
#endif
    const size_t bufferLen = (mLastVersion - mBase)/8 + 1;
    if (bufferLen > mVersionsLength) {
        unsigned char* newVersions = new unsigned char[mVersionsLength * 2];
        memset(newVersions, 0, mVersionsLength*2);
        memcpy(newVersions, mVersions, mVersionsLength);
        mVersionsLength *= 2;
        delete[] mVersions;
        mVersions = newVersions;
    }
    std::unique_ptr<unsigned char[]> resBuffer(new unsigned char[bufferLen + 16]);
    memcpy(resBuffer.get(), &mBase, sizeof(mBase));
    memcpy(resBuffer.get() + 8, &mLowestActiveVersion, sizeof(mLowestActiveVersion));
    memcpy(resBuffer.get() + 16, mVersions, bufferLen);
    auto version = ++mLastVersion;
    if (mActiveBaseVersionsCount <= mBase - mLowestActiveVersion) {
        auto n = new uint32_t[mActiveBaseVersionsCount*2];
        memset(n, 0, mActiveBaseVersionsCount * 2 * sizeof(uint32_t));
        memcpy(n, mActiveBaseVersions, mActiveBaseVersionsCount*sizeof(uint32_t));
        delete[] mActiveBaseVersions;
        mActiveBaseVersions = n;
        mActiveBaseVersionsCount *= 2;
    }
    mActiveBaseVersions[mBase - mLowestActiveVersion] += 1;
    return store::SnapshotDescriptor(resBuffer.release(), bufferLen + 16, version);
}

void DummyManager::abortTx(const SnapshotDescriptor& version) {
    commitTx(version);
}

void DummyManager::commitTx(const SnapshotDescriptor& v) {
    auto version = v.version();
    Lock _(mMutex);
#ifndef NDEBUG
    --mRunningTransactions;
#endif
    unsigned char byteIdx = (unsigned char)(1 << ((version - mBase - 1) % 8));
    mVersions[(version - mBase - 1)/8] |= byteIdx;
    size_t moveIdx = 0u;
    while (mVersions[moveIdx] == (unsigned char)(0xff)) {
        ++moveIdx;
    }
    if (moveIdx != 0) {
        mBase += 8*moveIdx;
        auto last = (mLastVersion - mBase - 1)/8 + 1;
        memmove(mVersions, mVersions + moveIdx, last);
        memset(mVersions + last, 0, mVersionsLength - last);
    }
    // handle lowest active version
    mActiveBaseVersions[v.baseVersion() - mLowestActiveVersion] -= 1;
    size_t moveCount = 0u;
    while (mActiveBaseVersions[moveCount] == 0 && mLowestActiveVersion < std::max(decltype(mBase)(1), mBase) - 1) {
        ++mLowestActiveVersion;
        ++moveCount;
    }
    if (moveCount != 0) {
        memmove(mActiveBaseVersions, mActiveBaseVersions + moveCount, (mBase - moveCount)*sizeof(uint32_t));
        memset(mActiveBaseVersions + mBase, 0, mActiveBaseVersionsCount - mBase);
    }
}

uint64_t DummyManager::getLowestActiveVersion() {
    return mLowestActiveVersion;
}
} // namespace store
} // namespace tell
