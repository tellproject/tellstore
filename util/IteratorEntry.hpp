#pragma once

#include <cstdint>

namespace tell {
namespace store {

class Record;

class BaseIteratorEntry {
public:
    uint64_t mValidFrom = 0;
    uint64_t mValidTo = 0;
    const char* mData = nullptr;
    uint64_t mSize = 0;
    const Record* mRecord = nullptr;

    uint64_t validFrom() const {
        return mValidFrom;
    }

    uint64_t validTo() const {
        return mValidTo;
    }

    const char* data() const {
        return mData;
    }

    uint64_t size() const {
        return mSize;
    }

    const Record* record() const {
        return mRecord;
    }
};

} // namespace store
} // namespace tell
