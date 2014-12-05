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

extern void free(void* ptr);

extern void free_now(void* ptr);

class allocator {
    std::atomic<uint64_t>* cnt_;
    static std::mutex mutex_;
public:
    allocator();

    ~allocator();

    static void* malloc(std::size_t size);

    static void free(void* ptr, std::function<void()> destruct = []() {
    });

    static void free_now(void* ptr);
};

template<typename T>
static void mark_for_deletion(T* ptr) {
    if (ptr)
        allocator::free(ptr, [ptr]() {
            ptr->~T();
        });
}

template<typename T>
class object_allocator {
public:
    typedef T value_type;
    typedef T* pointer;
    typedef const T* const_pointer;
    typedef T& reference;
    typedef const T& const_reference;
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;
    template<typename U>
    struct rebind {
        typedef object_allocator<U> other;
    };
public:
    pointer address(reference x) const {
        return reinterpret_cast<pointer>(&reinterpret_cast<uint8_t&>(x));
    }

    pointer allocate(size_type n, void* hint = 0) {
        return reinterpret_cast<pointer>(allocator::malloc(n * sizeof(T)));
    }

    void deallocate(pointer p, size_type n) {
        allocator::free(p);
    }

    size_type max_size() const {
        return std::numeric_limits<size_type>::max();
    }

    void construct(pointer p, const_reference val) {
        new(reinterpret_cast<void*>(p)) T(val);
    }

    template<typename U, typename... Args>
    void construct(U* p, Args&& ... args) {
        new(reinterpret_cast<void*>(p)) U(std::forward<Args>(args)...);
    }

    void destroy(pointer p) {
        p->~T();
    }

    template<typename U>
    void destroy(U* p) {
        p->~U();
    }
};

template<typename T1, typename T2>
bool operator==(const object_allocator<T1>&, const object_allocator<T2>&) {
    return true;
}

template<typename T1, typename T2>
bool operator!=(const object_allocator<T1>&, const object_allocator<T2>&) {
    return true;
}

class object {
    void* operator new(std::size_t size) {
        return allocator::malloc(size);
    }

    void* operator new[](std::size_t size) {
        return allocator::malloc(size);
    }

    void operator delete(void* ptr) {
        allocator::free(ptr);
    }

    void operator delete[](void* ptr) {
        allocator::free(ptr);
    }
};

} // namespace store
} // namespace tell
