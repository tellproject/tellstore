#pragma once

#include "SnapshotDescriptor.hpp"
#include <cstdint>
#include <mutex>
#include <vector>

namespace tell {
namespace store {

class DummyManager {
    static constexpr size_t INITAL_BUFFER_SIZE = 1024;
    unsigned char* mVersions;
    size_t mVersionsLength;
    uint64_t mBase;
    uint64_t mLastVersion;
    uint64_t mLowestActiveVersion;
    uint32_t* mActiveBaseVersions;
    size_t mActiveBaseVersionsCount;
#ifndef NDEBUG
    size_t mRunningTransactions;
#endif
    mutable std::mutex mMutex;
    using Lock = std::lock_guard<std::mutex>;
public:
    DummyManager();
    ~DummyManager();
public: // API
    SnapshotDescriptor startTx();
    void abortTx(const SnapshotDescriptor& version);
    void commitTx(const SnapshotDescriptor& version);
    uint64_t getLowestActiveVersion();
};

using CommitManager = DummyManager;

} // namespace store
} // namespace tell
