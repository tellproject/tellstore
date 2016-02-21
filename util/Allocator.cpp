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

#include "Allocator.hpp"

#include <algorithm>

namespace tell {
namespace store {

MemoryConsumer::~MemoryConsumer() {
    LOG_ASSERT(state.load() == MemoryConsumerState::INACTIVE, "Consumer must be inactive when destroying");
    reclaimer.removeConsumer(this);
}

std::unique_ptr<MemoryConsumer> MemoryReclaimer::createConsumer() {
    std::unique_ptr<MemoryConsumer> consumer(new MemoryConsumer(*this));

    std::unique_lock<decltype(mConsumerMutex)> _(mConsumerMutex);
    mConsumer.emplace_back(consumer.get());

    return std::move(consumer);
}

void MemoryReclaimer::wait() {
    std::unique_lock<decltype(mConsumerMutex)> _(mConsumerMutex);

    std::vector<MemoryConsumer*> waitList;
    waitList.reserve(mConsumer.size());

    for (auto consumer : mConsumer) {
        auto state = MemoryConsumerState::ACTIVE;
        consumer->state.compare_exchange_strong(state, MemoryConsumerState::WAITING);

        // Enqueue the consumer if we have to wait for it:
        // - The consumer was in ACTIVE state and the compare and swap succeeded (now in WAITING)
        // - The consumer was already in the WAITING state and the compare and swap failed
        if (state != MemoryConsumerState::INACTIVE) {
            waitList.emplace_back(consumer);
        }
    }

    for (auto consumer : waitList) {
        while (consumer->state.load() == MemoryConsumerState::WAITING);
    }
}

void MemoryReclaimer::removeConsumer(MemoryConsumer* consumer) {
    std::unique_lock<decltype(mConsumerMutex)> _(mConsumerMutex);
    auto i = std::find(mConsumer.begin(), mConsumer.end(), consumer);
    if (i == mConsumer.end()) {
        return;
    }
    mConsumer.erase(i);
}

} // namespace store
} // namespace tell
