#pragma once

#include <cstdint>
#include <atomic>
#include <limits>
#include <utility>
#include <functional>
#include <mutex>

namespace tell {
namespace store {

constexpr const unsigned NUM_LISTS = 64;

extern void init();

extern void destroy();

extern void* malloc(std::size_t size);

extern void* malloc(std::size_t size, std::size_t align);

extern void free(void* ptr);

extern void free_now(void* ptr);

class allocator {
    std::atomic<uint64_t>* cnt_;
    static std::mutex mutex_;
public:
    allocator();

    ~allocator();

    static void* malloc(std::size_t size);

    static void* malloc(std::size_t size, std::size_t align);

    static void free(void* ptr, std::function<void()> destruct = []() { });
    static void free_in_order(void* ptr, std::function<void()> destruct = []() { });

    static void free_now(void* ptr);
};

template<typename T>
static void mark_for_deletion(T* ptr) {
    if (ptr)
        allocator::free(ptr, [ptr]() {
            ptr->~T();
        });
}

} // namespace store
} // namespace tell
