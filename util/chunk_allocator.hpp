/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#pragma once

#include <vector>
#include <array>

namespace crossbow {

template<size_t ChunkSize, size_t Align = 4>
class chunk {
    std::array<char, ChunkSize> mChunk;
    size_t pos = 0;
public:
    void* allocate(size_t size) {
        size += 4 - (size % 4);
        if (pos + size >= mChunk.size())
            return nullptr;
        auto res = mChunk.data() + pos;
        pos += size;
        return res;
    }
};

template<size_t ChunkSize = 0x800000, size_t Align = 4, class Allocator = std::allocator<chunk<ChunkSize, Align>>>
class chunk_allocator {
    using Chunk = chunk<ChunkSize, Align>;
    std::vector<Chunk*> mChunks;
    Allocator allocator;
public:
    chunk_allocator(Allocator allocator = Allocator())
            : allocator(allocator)
    {
        auto c = allocator.allocate(1);
        allocator.template construct<Chunk>(c);
        mChunks.push_back(c);
    }
    ~chunk_allocator() {
        for (auto c : mChunks) {
            allocator.destroy(c);
            allocator.deallocate(c, 1);
        }
    }
public: // Allocation and deallocation
    void* alloc(size_t n) {
        if (n > ChunkSize) {
            throw std::bad_alloc();
        }
        void* res = mChunks.back()->allocate(n);
        if (res == nullptr) {
            auto c = allocator.allocate(1);
            allocator.template construct<Chunk>(c);
            mChunks.push_back(c);
        }
        return mChunks.back()->allocate(n);
    }
    size_t max_size() const {
        return ChunkSize;
    }
};

template<class T, class Alloc = chunk_allocator<>>
class copy_allocator {
public:
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    //template<class U>
    //struct rebind {
    //    using other = copy_allocator<U, Alloc>;
    //};
    template<class U>
    using rebind = copy_allocator<U, Alloc>;
    Alloc& allocator;
public:
    copy_allocator(Alloc& allocator) : allocator(allocator) {}
    template<class U>
    copy_allocator(const copy_allocator<U, Alloc>& o)
        : allocator(o.allocator)
    {}
    copy_allocator& operator= (copy_allocator&) {
        return *this;
    }
    size_type max_size() const {
        return allocator.max_size();
    }
    pointer address(reference x) const {
        char& ref = reinterpret_cast<char&>(x);
        return reinterpret_cast<pointer>(&ref);
    }
    const_pointer address(const_reference x) const {
        const char& ref = reinterpret_cast<char&>(x);
        return reinterpret_cast<const_pointer>(&ref);
    }
    pointer allocate(size_type n, const void*  = nullptr) {
        return reinterpret_cast<pointer>(allocator.alloc(n*sizeof(value_type)));
    }
    void deallocate(pointer, size_type) {
    }
    template<class U, class... Args>
    void construct(U* p, Args&&... args) {
        ::new ((void*)p) U(std::forward<Args>(args)...);
    }
    template<class U>
    void destroy(U* p) {
        p->~U();
    }
};

} // crossbow
