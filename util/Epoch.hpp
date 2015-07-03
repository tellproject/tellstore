#pragma once

#include <cstdint>
#include <atomic>
#include <limits>
#include <utility>
#include <functional>
#include <mutex>
#include <type_traits>

namespace tell {
namespace store {

constexpr const unsigned NUM_LISTS = 64;

extern void init();

extern void destroy();

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

    static void invoke(std::function<void()> fun) {
        allocator::free(allocator::malloc(0), std::move(fun));
    }

    template <typename T, typename... Args>
    static typename std::enable_if<alignof(T) == alignof(void*), T*>::type construct(Args&&... args) {
        return new (allocator::malloc(sizeof(T))) T(std::forward<Args>(args)...);
    }

    template <typename T, typename... Args>
    static typename std::enable_if<(alignof(T) > alignof(void*)) && !(alignof(T) & (alignof(T) - 1)), T*>::type
            construct(Args&&... args) {
        return new (allocator::malloc(sizeof(T), alignof(T))) T(std::forward<Args>(args)...);
    }

    template <typename T>
    static void destroy(T* ptr) {
        if (!ptr) {
            return;
        }

        allocator::free(ptr, [ptr] () {
            ptr->~T();
        });
    }

    template <typename T>
    static void destroy_in_order(T* ptr) {
        if (!ptr) {
            return;
        }

        allocator::free_in_order(ptr, [ptr] () {
            ptr->~T();
        });
    }

    template <typename T>
    static void destroy_now(T* ptr) {
        if (!ptr) {
            return;
        }

        ptr->~T();
        allocator::free_now(ptr);
    }
};

} // namespace store
} // namespace tell
