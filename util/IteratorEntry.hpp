#pragma once
#include <cstdint>

namespace tell {
namespace store {

class Record;

struct IteratorEntry {
    uint64_t validFrom;
    uint64_t validTo;
    const char* data;
    const Record* record;
};

} // namespace store
} // namespace tell
