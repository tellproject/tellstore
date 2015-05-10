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

    const Record* record() const {
        return mRecord;
    }
};

} // namespace store
} // namespace tell
