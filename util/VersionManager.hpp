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

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/non_copyable.hpp>

#include <atomic>
#include <cstdint>

namespace tell {
namespace store {

class VersionManager : crossbow::non_copyable, crossbow::non_movable {
public:
    VersionManager()
            : mLowestActiveVersion(0x1u) {
    }

    uint64_t lowestActiveVersion() const {
        return mLowestActiveVersion.load();
    }

    void addSnapshot(const commitmanager::SnapshotDescriptor& snapshot) {
        auto lowestActiveVersion = mLowestActiveVersion.load();
        while (lowestActiveVersion < snapshot.lowestActiveVersion()) {
            if (!mLowestActiveVersion.compare_exchange_strong(lowestActiveVersion, snapshot.lowestActiveVersion())) {
                continue;
            }
            return;
        }
    }

private:
    std::atomic<uint64_t> mLowestActiveVersion;
};

} // namespace store
} // namespace tell
