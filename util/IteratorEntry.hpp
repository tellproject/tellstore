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

#include <cstdint>

namespace tell {
namespace store {

class Record;

class BaseIteratorEntry {
public:
    uint64_t mValidFrom = 0;
    uint64_t mValidTo = 0;
    const char* mData = nullptr;
    uint64_t mSize = 0;
    const Record* mRecord = nullptr;

    uint64_t validFrom() const {
        return mValidFrom;
    }

    uint64_t validTo() const {
        return mValidTo;
    }

    const char* data() const {
        return mData;
    }

    uint64_t size() const {
        return mSize;
    }

    const Record* record() const {
        return mRecord;
    }
};

} // namespace store
} // namespace tell
