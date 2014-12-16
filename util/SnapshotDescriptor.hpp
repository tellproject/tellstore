#pragma once

#include <cstdint>
#include <cstddef>

namespace tell {
namespace store {

struct SnapshotDescriptor {
private:
    uint64_t mVersion;
    unsigned char* mDescriptor;
    size_t mLength;
public:
    SnapshotDescriptor(unsigned char* desc, size_t len, uint64_t version)
        : mVersion(version), mDescriptor(desc), mLength(len) {
    }

    ~SnapshotDescriptor() {
        if (mDescriptor) delete mDescriptor;
    }

    SnapshotDescriptor(const SnapshotDescriptor&) = delete;

    SnapshotDescriptor(SnapshotDescriptor&& o)
        : mDescriptor(o.mDescriptor), mLength(o.mLength) {
        o.mDescriptor = nullptr;
    }

    SnapshotDescriptor& operator=(const SnapshotDescriptor&) = delete;

    SnapshotDescriptor& operator=(SnapshotDescriptor&& o) {
        delete[] mDescriptor;
        mDescriptor = o.mDescriptor;
        o.mDescriptor = nullptr;
        mLength = o.mLength;
        return *this;
    }

    uint64_t version() const {
        return mVersion;
    }

    uint64_t baseVersion() const {
        return *reinterpret_cast<uint64_t*>(mDescriptor);
    }

    uint64_t lowestActiveVersion() const {
        return *(reinterpret_cast<const uint64_t*>(mDescriptor) + 1);
    }

    bool inReadSet(uint64_t version) const {
        auto base = baseVersion();
        if (base >= version)
            return true;
        if ((mLength - 16) * 8 > version - base) {
            // in this case, the version is not in the
            // mDescriptor -> false
            return false;
        }
        unsigned char byteIdx = (unsigned char) (1 << (8 - ((version - base) % 8)));
        return mDescriptor[16 + (version - base) / 8] & byteIdx;
    }
};

} // namespace store
} // namespace tell
