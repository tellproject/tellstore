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

#include "RowStoreRecord.hpp"

#include <tellstore/ErrorCode.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/logger.hpp>

#include <type_traits>

namespace tell {
namespace store {
namespace deltamain {

RowStoreMainEntry* RowStoreMainEntry::serialize(void* ptr, uint64_t key, const std::vector<RecordHolder>& elements) {
    auto entry = new (ptr) RowStoreMainEntry(key, elements.size());

    auto versions = entry->versionData();
    auto offsets = entry->offsetData();
    offsets[0] = serializedHeaderSize(elements.size());
    for (decltype(elements.size()) i = 0; i < elements.size(); ++i) {
        auto& element = elements[i];
        versions[i] = element.version;
        offsets[i + 1] = offsets[i] + element.size;
        memcpy(reinterpret_cast<char*>(entry) + offsets[i], element.data, element.size);
    }
    return entry;
}

RowStoreMainEntry* RowStoreMainEntry::serialize(void* ptr, const RowStoreMainEntry* oldEntry, uint32_t oldSize) {
    LOG_ASSERT(oldSize == oldEntry->offsetData()[oldEntry->versionCount], "Calculated size does not match");

    // Can not copy the complete record at once as the next field can be modified in the meantime
    // First create a new header and then copy all immutable data fields.
    auto offsets = oldEntry->offsetData();
    auto entry = new (ptr) RowStoreMainEntry(oldEntry->key, oldEntry->versionCount);
    memcpy(entry->data(), oldEntry->data(), offsets[oldEntry->versionCount] - sizeof(RowStoreMainEntry));
    return entry;
}

template <typename T>
int RowStoreRecordImpl<T>::get(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot,
        size_t& size, const char*& data, uint64_t& version, bool& isNewest) const {
    auto versions = mEntry->versionData();

    // Skip elements already overwritten by an element in the update log
    typename std::remove_const<decltype(mEntry->versionCount)>::type i = 0;
    for (; i < mEntry->versionCount && versions[i] >= highestVersion; ++i) {
    }
    for (; i < mEntry->versionCount; ++i) {
        if (!snapshot.inReadSet(versions[i])) {
            isNewest = false;
            continue;
        }

        version = versions[i];

        auto offsets = mEntry->offsetData();
        size = offsets[i + 1] - offsets[i];
        if (size == 0) {
            return (isNewest ? error::not_found : error::not_in_snapshot);
        }

        data = reinterpret_cast<const char*>(mEntry) + offsets[i];
        return 0;
    }
    return (isNewest ? error::not_found : error::not_in_snapshot);
}

template <typename T>
bool RowStoreRecordImpl<T>::needsCleaning(uint64_t minVersion) const {
    // In case the record has pending updates it needs to be cleaned
    if (mNewest != 0u) {
        return true;
    }
    // The record needs cleaning if the last version can be purged
    auto versions = mEntry->versionData();
    return (versions[mEntry->versionCount - 1] < minVersion);
}

template <typename T>
void RowStoreRecordImpl<T>::collect(uint64_t minVersion, uint64_t highestVersion,
        std::vector<RecordHolder>& elements) const {
    // Check if the oldest element is the oldest readable element
    if (highestVersion <= minVersion) {
        return;
    }

    auto versions = mEntry->versionData();
    auto offsets = mEntry->offsetData();

    // Skip elements already overwritten by an element in the update log
    typename std::remove_const<decltype(mEntry->versionCount)>::type i = 0;
    for (; i < mEntry->versionCount && versions[i] >= highestVersion; ++i) {
    }

    // Append all valid elements newer than the lowest active version (if they exist)
    for (; i < mEntry->versionCount && versions[i] > minVersion; ++i) {
        elements.emplace_back(versions[i], reinterpret_cast<const char*>(mEntry) + offsets[i],
                offsets[i + 1] - offsets[i]);
    }

    // Append the newest element that is older or equal the lowest active version (if it exists)
    if (i < mEntry->versionCount) {
        elements.emplace_back(versions[i], reinterpret_cast<const char*>(mEntry) + offsets[i],
                offsets[i + 1] - offsets[i]);
    }
}

int RowStoreRecord::canUpdate(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot,
        RecordType type) const {
    auto versions = mEntry->versionData();

    // Skip elements already overwritten by an element in the update log
    typename std::remove_const<decltype(mEntry->versionCount)>::type i = 0;
    for (; i < mEntry->versionCount && versions[i] >= highestVersion; ++i) {
    }

    // All elements were overwritten by the update log, behave as if no element was written
    if (i >= mEntry->versionCount) {
        return (type == RecordType::DELETE ? 0 : error::invalid_write);
    }
    if (!snapshot.inReadSet(versions[i])) {
        return error::not_in_snapshot;
    }

    auto offsets = mEntry->offsetData();
    auto size = offsets[i + 1] - offsets[i];
    auto actualType = (size == 0 ? RecordType::DELETE : RecordType::DATA);
    return (type == actualType ? 0 : error::invalid_write);
}

int RowStoreRecord::canRevert(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot, bool& needsRevert) const {
    auto versions = mEntry->versionData();

    // Skip elements already overwritten by an element in the update log
    typename std::remove_const<decltype(mEntry->versionCount)>::type i = 0;
    for (; i < mEntry->versionCount && versions[i] >= highestVersion; ++i) {
    }

    if (i >= mEntry->versionCount) {
        needsRevert = false;
        return 0;
    }

    if (versions[i] < snapshot.version()) {
        needsRevert = false;
        return 0;
    }
    // Check if version history has element
    if (versions[i] > snapshot.version()) {
        needsRevert = false;
        for (; i < mEntry->versionCount; ++i) {
            if (versions[i] < snapshot.version()) {
                return 0;
            }
            if (versions[i] == snapshot.version()) {
                return error::not_in_snapshot;
            }
        }
        return 0;
    }

    needsRevert = true;
    return 0;
}

template class RowStoreRecordImpl<const RowStoreMainEntry*>;
template class RowStoreRecordImpl<RowStoreMainEntry*>;

} // namespace deltamain
} // namespace store
} // namespace tell
