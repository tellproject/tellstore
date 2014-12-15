#include "CommitManager.hpp"

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
{
    memset(mVersions, 0, INITAL_BUFFER_SIZE);
}

DummyManager::~DummyManager() {
    delete[] mVersions;
}

SnapshotDescriptor DummyManager::startTx() {
    Lock _(mMutex);
    const size_t bufferLen = (mLastVersion - mBase)/8 + 1 + 16;
    if (bufferLen - 16 > mVersionsLength) {
        unsigned char* newVersions = new unsigned char[mVersionsLength * 2];
        memset(newVersions, 0, mVersionsLength*2);
        memcpy(newVersions, mVersions, mVersionsLength);
        mVersionsLength *= 2;
        delete[] mVersions;
        mVersions = newVersions;
    }
    std::unique_ptr<unsigned char[]> resBuffer(new unsigned char[bufferLen]);
    memcpy(resBuffer.get(), &mBase, sizeof(mBase));
    memcpy(resBuffer.get() + 8, &mLowestActiveVersion, 8);
    memcpy(resBuffer.get() + 16, mVersions, bufferLen - 8);
    auto version = ++mLastVersion;
    if (mActiveBaseVersionsCount < mBase - mLowestActiveVersion) {
        auto n = new uint32_t[mActiveBaseVersionsCount*2];
        memcpy(n, mActiveBaseVersions, mActiveBaseVersionsCount*sizeof(uint32_t));
        delete[] mActiveBaseVersions;
        mActiveBaseVersions = n;
        mActiveBaseVersionsCount *= 2;
    }
    mActiveBaseVersions[mBase] += 1;
    return store::SnapshotDescriptor(resBuffer.get(), bufferLen, version);
}

void DummyManager::abortTx(const SnapshotDescriptor& version) {
    commitTx(version);
}

void DummyManager::commitTx(const SnapshotDescriptor& v) {
    auto version = v.version();
    Lock _(mMutex);
    unsigned char byteIdx = (unsigned char)(1 << (version - mBase));
    mVersions[(version - mBase)/8] |= byteIdx;
    size_t moveIdx = 0u;
    while (mVersions[moveIdx] == (unsigned char)(0xff)) {
        ++moveIdx;
    }
    if (moveIdx != 0) {
        mBase += 8*moveIdx;
        auto last = (mLastVersion - mBase - 1)/8 + 1;
        memmove(mVersions, mVersions + moveIdx, last);
        memset(mVersions + last, 0, INITAL_BUFFER_SIZE - last);
    }
    // handle lowest active version
    mActiveBaseVersions[v.lowestActiveVersion() - mLastVersion] -= 1;
    size_t moveCount = 0u;
    while (mActiveBaseVersions[0] == 0 && mLowestActiveVersion < mBase) {
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
