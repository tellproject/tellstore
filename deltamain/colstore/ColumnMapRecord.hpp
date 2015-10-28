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

#include <deltamain/Record.hpp>

#include <crossbow/enum_underlying.hpp>

#include <atomic>
#include <cstdint>

namespace tell {
namespace commitmanager {

class SnapshotDescriptor;

} // namespace commitmanager

namespace store {
namespace deltamain {

class ColumnMapContext;
class ColumnMapMainPage;

/**
 * @brief Struct storing information about a single element in the column map page
 */
struct alignas(8) ColumnMapMainEntry {
public:
    ColumnMapMainEntry(uint64_t _key, uint64_t _version)
            : key(_key),
              version(_version),
              newest(0x0u) {
    }

    const uint64_t key;
    const uint64_t version;
    std::atomic<uintptr_t> newest;
};

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

    int get(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot, size_t& size, const char*& data,
            uint64_t& version, bool& isNewest) const;

protected:
    T mEntry;
    uintptr_t mNewest;
    const ColumnMapContext& mContext;

private:
    void materialize(const ColumnMapMainPage* page, uint64_t idx, char* data, size_t size) const;
};

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
