#pragma once

#include <cstdint>
#include <config.h>

namespace tell {
namespace store {
struct StorageConfig {
    uint16_t gcIntervall = 60;
    size_t totalMemory = TOTAL_MEMORY;
};
} // namespace store
} // namespace tell
