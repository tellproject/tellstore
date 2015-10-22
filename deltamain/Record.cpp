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

#include "Record.hpp"

#include <tellstore/ErrorCode.hpp>

#include <util/Log.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/logger.hpp>

#include <limits>

namespace tell {
namespace store {
namespace deltamain {

template <typename T>
int InsertRecordImpl<T>::get(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot,
        size_t& size, const char*& data, uint64_t& version, bool& isNewest) const {
    // Check if the element was already overwritten by an element in the update log
    if (mEntry->version >= highestVersion) {
        return error::not_found;
    }
    if (!snapshot.inReadSet(mEntry->version)) {
        isNewest = false;
        return error::not_in_snapshot;
    }

    auto logEntry = LogEntry::entryFromData(reinterpret_cast<const char*>(mEntry));

    version = mEntry->version;
    data = mEntry->data();
    size = logEntry->size() - sizeof(InsertLogEntry);
    return 0;
}

template <typename T>
void InsertRecordImpl<T>::collect(uint64_t minVersion, uint64_t highestVersion, std::vector<RecordHolder>& elements) const {
    // Check if the oldest element is the oldest readable element
    if (highestVersion <= minVersion) {
        return;
    }

    auto logEntry = LogEntry::entryFromData(reinterpret_cast<const char*>(mEntry));
    elements.emplace_back(mEntry->version, mEntry->data(), logEntry->size() - sizeof(InsertLogEntry));
}

int InsertRecord::canUpdate(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot,
        RecordType type) const {
    // Check if the element was already overwritten by an element in the update log
    if (mEntry->version >= highestVersion) {
        return (type == RecordType::DELETE ? 0 : error::invalid_write);
    }

    if (!snapshot.inReadSet(mEntry->version)) {
        return error::not_in_snapshot;
    }

    return (type == RecordType::DATA ? 0 : error::invalid_write);
}

int InsertRecord::canRevert(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot, bool& needsRevert) const {
    // The element only needs a revert if it was not overwritten by the update log and has the same version
    needsRevert = !(mEntry->version >= highestVersion || mEntry->version != snapshot.version());
    return 0;
}

UpdateRecordIterator::UpdateRecordIterator(const UpdateLogEntry* record, uint64_t baseVersion)
        : mCurrent(record),
          mBaseVersion(baseVersion),
          mLowestVersion(std::numeric_limits<decltype(mLowestVersion)>::max()) {
    if (!mCurrent) {
        return;
    }
    LOG_ASSERT(mCurrent->version >= mBaseVersion, "Version of element in Update Log must never be lower than "
            "base version");

    // Update lowest version
    mLowestVersion = mCurrent->version;

    // Forward version chain if the element was a revert
    auto logEntry = LogEntry::entryFromData(reinterpret_cast<const char*>(mCurrent));
    if (logEntry->type() == crossbow::to_underlying(RecordType::REVERT)) {
        next();
    }
}

void UpdateRecordIterator::next() {
    auto entry = mCurrent;
    while (true) {
        if (entry->version == mBaseVersion) {
            mCurrent = nullptr;
            return;
        }

        // Forward version chain until we see an element with smaller version number or we reached the end
        // This assumes that the version chain is sorted by version number which it is not: But the only case where
        // a newer version number can follow an older version number is when the newer version was reverted so it
        // can be ignored anyway.
        do {
            entry = reinterpret_cast<const UpdateLogEntry*>(entry->previous.load());
            if (!entry || entry->version < mBaseVersion) {
                mCurrent = nullptr;
                return;
            }
        } while (entry->version >= mLowestVersion);

        // Update lowest version
        mLowestVersion = entry->version;

        // Forward version chain if the element was a revert
        auto logEntry = LogEntry::entryFromData(reinterpret_cast<const char*>(entry));
        if (logEntry->type() == crossbow::to_underlying(RecordType::REVERT)) {
            continue;
        }
    }
    mCurrent = entry;
}

template class InsertRecordImpl<const InsertLogEntry*>;
template class InsertRecordImpl<InsertLogEntry*>;

} // namespace deltamain
} // namespace store
} // namespace tell
