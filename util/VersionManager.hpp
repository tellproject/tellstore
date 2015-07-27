#pragma once

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/non_copyable.hpp>

#include <atomic>
#include <cstdint>

namespace tell {
namespace store {

class VersionManager : crossbow::non_copyable, crossbow::non_movable {
public:
    VersionManager()
            : mLowestActiveVersion(0x1u) {
    }

    uint64_t lowestActiveVersion() const {
        return mLowestActiveVersion.load();
    }

    void addSnapshot(const commitmanager::SnapshotDescriptor& snapshot) {
        auto lowestActiveVersion = mLowestActiveVersion.load();
        while (lowestActiveVersion < snapshot.lowestActiveVersion()) {
            if (!mLowestActiveVersion.compare_exchange_strong(lowestActiveVersion, snapshot.lowestActiveVersion())) {
                continue;
            }
            return;
        }
    }

private:
    std::atomic<uint64_t> mLowestActiveVersion;
};

} // namespace store
} // namespace tell
