#pragma once
#include <cstdint>
#include <cstddef>

namespace tell {
namespace store {

class Page {
    char* mData;
public:
    Page(char* data) : mData(data) {}

    size_t usedMemory() const {
        return size_t(*reinterpret_cast<const uint64_t*>(mData));
    }

    char* gc() {
        return mData;
    }
};

} // namespace store
} // namespace tell

