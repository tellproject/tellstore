#pragma once

#include <cstdint>
#include <config.h>

namespace tell {
namespace store {
struct StorageConfig {
    uint16_t gcIntervall = 60;
    size_t totalMemory = TOTAL_MEMORY;
    int numScanThreads = 2;
    size_t hashMapCapacity = HASHMAP_CAPACITY;
};
} // namespace store
} // namespace tell
