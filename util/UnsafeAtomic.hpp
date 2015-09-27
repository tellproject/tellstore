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

#include <atomic>
#include <cstdint>
#include <type_traits>

namespace tell {
namespace store {

/**
 * @brief Unsafe 128 bit atomic
 *
 * Allows for atomic 128 bit compare and swap but loads and stores are implemented as two separate 64 bit operations.
 */
template <typename T>
class alignas(16) UnsafeAtomic {
public:
    static_assert(sizeof(T) == 16, "Only 16 byte types are supported");
    static_assert(alignof(T) == 16, "Only 16 byte aligned types are supported");

#if defined(__GNUC__) && __GNUC__ < 5
#else
    static_assert(std::is_trivially_copyable<T>::value, "Only trivially copyable types are supported");
#endif

    T unsafe_load() const noexcept {
        T result;
        __atomic_load(reinterpret_cast<const uint64_t*>(&mElement), reinterpret_cast<uint64_t*>(&result),
                std::memory_order_seq_cst);
        __atomic_load(reinterpret_cast<const uint64_t*>(&mElement) + 1, reinterpret_cast<uint64_t*>(&result) + 1,
                std::memory_order_seq_cst);
        return result;
    }

    void unsafe_store(T element) noexcept {
        __atomic_store(reinterpret_cast<uint64_t*>(&mElement), reinterpret_cast<uint64_t*>(&element),
                std::memory_order_seq_cst);
        __atomic_store(reinterpret_cast<uint64_t*>(&mElement) + 1, reinterpret_cast<uint64_t*>(&element) + 1,
                std::memory_order_seq_cst);
    }

    bool compare_exchange_strong(T& expected, T desired) noexcept {
        return __atomic_compare_exchange(&mElement, &expected, &desired, false, std::memory_order_seq_cst,
                std::memory_order_seq_cst);
    }

private:
    T mElement;
};

} // namespace store
} // namespace tell
