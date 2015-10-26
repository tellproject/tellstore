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

#include "ColumnMapRecord.hpp"

#include "ColumnMapContext.hpp"

#include <tellstore/ErrorCode.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

namespace tell {
namespace store {
namespace deltamain {

template <typename T>
int ColumnMapRecordImpl<T>::get(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot,
        size_t& size, const char*& data, uint64_t& version, bool& isNewest) const {
    auto page = mContext.pageFromEntry(mEntry);
    auto entries = page->entryData();
    for (auto i = ColumnMapContext::pageIndex(page, mEntry); i < page->count && entries[i].key == mEntry->key; ++i) {
        // Skip elements already overwritten by an element in the update log
        if (entries[i].version >= highestVersion) {
            continue;
        }

        // Check if the element is readable by the snapshot
        if (!snapshot.inReadSet(entries[i].version)) {
            isNewest = false;
            continue;
        }

        version = entries[i].version;

        auto recordSizes = page->sizeData();
        if (recordSizes[i] == 0) {
            return (isNewest ? error::not_found : error::not_in_snapshot);
        }
        size = recordSizes[i];

        // Deserialize
        // Acquire and release buffer through epoch mechanism so it will be freed after this epoch
        auto res = reinterpret_cast<char*>(crossbow::allocator::malloc(size));
        materialize(page, i, res, size);
        crossbow::allocator::free(res);
        data = res;

        return 0;
    }

    return (isNewest ? error::not_found : error::not_in_snapshot);
}

template <typename T>
void ColumnMapRecordImpl<T>::materialize(const ColumnMapMainPage* page, uint64_t idx, char* data, size_t size) const {
    LOG_ASSERT(size > 0, "Tuple must not be of size 0");

    auto destData = data;
    auto recordData = page->recordData();

    // Copy all fixed size fields including the header (null bitmap) if the record has one into the fill page
    for (auto fieldLength : mContext.fieldLengths()) {
        memcpy(destData, recordData + idx * fieldLength, fieldLength);
        destData += fieldLength;
        recordData += page->count * fieldLength;
    }

    // Copy all variable size fields in one batch
    if (mContext.varSizeFieldCount() != 0) {
        auto heapEntries = reinterpret_cast<const ColumnMapHeapEntry*>(recordData);
        auto length = static_cast<size_t>(data + size - destData);
        memcpy(destData, page->heapData() - heapEntries[idx].offset, length);
    }
}

int ColumnMapRecord::canUpdate(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot,
        RecordType type) const {
    auto page = mContext.pageFromEntry(mEntry);
    auto entries = page->entryData();
    for (auto i = ColumnMapContext::pageIndex(page, mEntry); i < page->count && entries[i].key == mEntry->key; ++i) {
        // Skip elements already overwritten by an element in the update log
        if (entries[i].version >= highestVersion) {
            continue;
        }

        if (!snapshot.inReadSet(entries[i].version)) {
            return error::not_in_snapshot;
        }

        auto recordSizes = page->sizeData();
        auto actualType = (recordSizes[i] == 0 ? RecordType::DELETE : RecordType::DATA);
        return (type == actualType ? 0 : error::invalid_write);
    }

    // All elements were overwritten by the update log, behave as if no element was written
    return (type == RecordType::DELETE ? 0 : error::invalid_write);
}

int ColumnMapRecord::canRevert(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot,
        bool& needsRevert) const {
    auto page = mContext.pageFromEntry(mEntry);
    auto entries = page->entryData();
    for (auto i = ColumnMapContext::pageIndex(page, mEntry); i < page->count && entries[i].key == mEntry->key; ++i) {
        // Skip elements already overwritten by an element in the update log
        if (entries[i].version >= highestVersion) {
            continue;
        }

        if (entries[i].version < snapshot.version()) {
            needsRevert = false;
            return 0;
        }

        // Check if version history has element
        if (entries[i].version > snapshot.version()) {
            needsRevert = false;
            for (; i < page->count && entries[i].key == mEntry->key; ++i) {
                if (entries[i].version < snapshot.version()) {
                    return 0;
                }
                if (entries[i].version == snapshot.version()) {
                    return error::not_in_snapshot;
                }
            }
            return 0;
        }

        needsRevert = true;
        return 0;
    }

    needsRevert = false;
    return 0;
}

template class ColumnMapRecordImpl<const ColumnMapMainEntry*>;
template class ColumnMapRecordImpl<ColumnMapMainEntry*>;

} // namespace deltamain
} // namespace store
} // namespace tell
