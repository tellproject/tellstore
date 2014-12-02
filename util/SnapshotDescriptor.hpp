#pragma once
#include <cstdint>
#include <cstddef>

namespace tell {
namespace store {

struct SnapshotDescriptor {
    char* descriptor;
    size_t length;
    SnapshotDescriptor(char* desc, size_t len)
            : descriptor(desc)
            , length(len) {}
    ~SnapshotDescriptor() {
        if (descriptor) delete descriptor;
    }
    SnapshotDescriptor(const SnapshotDescriptor&) = delete;
    SnapshotDescriptor(SnapshotDescriptor&& o)
            : descriptor(o.descriptor)
            , length(o.length)
    {
        o.descriptor = nullptr;
    }
    SnapshotDescriptor& operator= (const SnapshotDescriptor&) = delete;
    SnapshotDescriptor& operator= (SnapshotDescriptor&& o) {
        delete descriptor;
        descriptor = o.descriptor;
        o.descriptor = nullptr;
        length = o.length;
    }
    uint64_t baseVersion() const {
        return *reinterpret_cast<uint64_t*>(descriptor);
    }
    bool inReadSet(uint64_t version) {
        auto base = baseVersion();
        if (base >= version)
            return true;
        if ((length - 8) * 8 > version - base) {
            // in this case, the version is not in the
            // descriptor -> false
            return false;
        }
        unsigned char byteIdx = (unsigned char)(1 << (8 - ((version - base) % 8)));
        return descriptor[8 + (version - base)/8] & byteIdx;
    }
};

} // namespace store
} // namespace tell
