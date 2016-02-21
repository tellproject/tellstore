/*
 * (C) Copyright 2016 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
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

#include <crossbow/logger.hpp>
#include <crossbow/non_copyable.hpp>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <vector>

namespace tell {
namespace store {

class MemoryReclaimer;

enum class MemoryConsumerState : uint32_t {
    INACTIVE = 0u,
    ACTIVE,
    WAITING,
};

struct alignas(64) MemoryConsumer : crossbow::non_copyable, crossbow::non_movable {
    static void* operator new(size_t size) {
        LOG_ASSERT(size % alignof(MemoryConsumer) == 0u, "Size must be multiple of alignment");
        return ::aligned_alloc(alignof(MemoryConsumer), size);
    }

    static void operator delete(void* ptr) {
        ::free(ptr);
    }

    MemoryConsumer(MemoryReclaimer& _reclaimer)
            : reclaimer(_reclaimer),
              state(MemoryConsumerState::INACTIVE) {
    }

    ~MemoryConsumer();

    MemoryReclaimer& reclaimer;
    std::atomic<MemoryConsumerState> state;
};

class MemoryConsumerLock : crossbow::non_copyable, crossbow::non_movable {
public:
    MemoryConsumerLock(MemoryConsumer* consumer)
            : mConsumer(consumer) {
        mConsumer->state.store(MemoryConsumerState::ACTIVE);
    }

    ~MemoryConsumerLock() {
        if (mConsumer) {
            mConsumer->state.store(MemoryConsumerState::INACTIVE);
        }
    }

    void release() {
        mConsumer->state.store(MemoryConsumerState::INACTIVE);
        mConsumer = nullptr;
    }

private:
    MemoryConsumer* mConsumer;
};

/**
 * @brief Simple memory reclamation mechanism
 *
 * Each thread that wants to access shared data structures must create its own consumer. All memory operations must be
 * protected by locking the consumer object. Threads releasing memory must wait until all consumer have been unlocked
 * at least once.
 */
class MemoryReclaimer : crossbow::non_copyable, crossbow::non_movable {
public:
    /**
     * @brief Create a new consumer registered with this reclaimer
     */
    std::unique_ptr<MemoryConsumer> createConsumer();

    /**
     * @brief Wait until all registered consumers have been inactive at least once
     */
    void wait();

private:
    friend class MemoryConsumer;

    void removeConsumer(MemoryConsumer* consumer);

    std::mutex mConsumerMutex;
    std::vector<MemoryConsumer*> mConsumer;
};

} // namespace store
} // namespace tell
