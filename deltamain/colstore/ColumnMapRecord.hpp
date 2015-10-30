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

#include "ColumnMapContext.hpp"
#include "ColumnMapPage.hpp"

#include <deltamain/Record.hpp>

#include <tellstore/ErrorCode.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/enum_underlying.hpp>

#include <atomic>
#include <cstdint>

namespace tell {
namespace store {
namespace deltamain {

template <typename T>
class ColumnMapRecordImpl {
public:
    ColumnMapRecordImpl(T entry, const ColumnMapContext& context)
            : mEntry(entry),
              mNewest(mEntry->newest.load()),
              mContext(context) {
    }

    uint64_t key() const {
        return mEntry->key;
    }

    bool valid() const {
        return (mNewest & crossbow::to_underlying(NewestPointerTag::INVALID)) == 0;
    }

    T value() const {
        return mEntry;
    }

    uintptr_t newest() const {
        return mNewest;
    }

    uint64_t baseVersion() const {
        return mEntry->version;
    }

    template <typename Fun>
    int get(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot, Fun fun, bool isNewest) const;

protected:
    T mEntry;
    uintptr_t mNewest;
    const ColumnMapContext& mContext;
};

template <typename T>
template <typename Fun>
int ColumnMapRecordImpl<T>::get(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot, Fun fun,
        bool isNewest) const {
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


        auto recordSizes = page->sizeData();
        if (recordSizes[i] == 0) {
            return (isNewest ? error::not_found : error::not_in_snapshot);
        }

        auto dest = fun(recordSizes[i], entries[i].version, isNewest);
        mContext.materialize(page, i, dest, recordSizes[i]);
        return 0;
    }

    return (isNewest ? error::not_found : error::not_in_snapshot);
}

extern template class ColumnMapRecordImpl<const ColumnMapMainEntry*>;

class ConstColumnMapRecord : public ColumnMapRecordImpl<const ColumnMapMainEntry*> {
    using Base = ColumnMapRecordImpl<const ColumnMapMainEntry*>;
public:
    using Base::Base;

    ConstColumnMapRecord(const void* record, const ColumnMapContext& context)
            : Base(reinterpret_cast<const ColumnMapMainEntry*>(record), context) {
    }
};

extern template class ColumnMapRecordImpl<ColumnMapMainEntry*>;

class ColumnMapRecord : public ColumnMapRecordImpl<ColumnMapMainEntry*> {
    using Base = ColumnMapRecordImpl<ColumnMapMainEntry*>;
public:
    using Base::Base;

    ColumnMapRecord(void* record, const ColumnMapContext& context)
            : Base(reinterpret_cast<ColumnMapMainEntry*>(record), context) {
    }

    bool tryUpdate(uintptr_t value) {
        return mEntry->newest.compare_exchange_strong(mNewest, value);
    }

    bool tryInvalidate() {
        return mEntry->newest.compare_exchange_strong(mNewest, crossbow::to_underlying(NewestPointerTag::INVALID));
    }

    int canUpdate(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot, RecordType type) const;

    int canRevert(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot, bool& needsRevert) const;
};

} // namespace deltamain
} // namespace store
} // namespace tell
