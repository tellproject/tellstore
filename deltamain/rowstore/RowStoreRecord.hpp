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

#include <config.h>

#include <deltamain/Record.hpp>

#include <tellstore/ErrorCode.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/alignment.hpp>

#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <vector>

namespace tell {
namespace store {
namespace deltamain {

class RowStoreContext;

struct alignas(8) RowStoreMainEntry {
    static uint32_t serializedHeaderSize(uint64_t versionCount) {
        // Size of header
        auto size = static_cast<uint32_t>(sizeof(RowStoreMainEntry));

        // Size of version and offset array (including end offset)
        size += versionCount * (sizeof(uint64_t) + sizeof(uint32_t)) + sizeof(uint32_t);

        // 8 byte alignment
        return crossbow::align(size, 8u);
    }

    static uint32_t serializedSize(const std::vector<RecordHolder>& elements) {
        // Size of header
        auto size = serializedHeaderSize(elements.size());

        // Size of elements
        for (auto& i : elements) {
            size += i.size;
        }
        return size;
    }

    static RowStoreMainEntry* serialize(void* ptr, uint64_t key, const std::vector<RecordHolder>& elements);

    static RowStoreMainEntry* serialize(void* ptr, const RowStoreMainEntry* oldEntry, uint32_t oldSize);

    RowStoreMainEntry(uint64_t k, uint64_t vc)
            : key(k),
              versionCount(vc),
              newest(0x0u) {
    }

    const uint64_t* versionData() const {
        return reinterpret_cast<const uint64_t*>(data());
    }

    uint64_t* versionData() {
        return const_cast<uint64_t*>(const_cast<const RowStoreMainEntry*>(this)->versionData());
    }

    const uint32_t* offsetData() const {
        return reinterpret_cast<const uint32_t*>(data() + versionCount * sizeof(uint64_t));
    }

    uint32_t* offsetData() {
        return const_cast<uint32_t*>(const_cast<const RowStoreMainEntry*>(this)->offsetData());
    }

    const uint64_t key;
    const uint64_t versionCount;
    std::atomic<uintptr_t> newest;

private:
    const char* data() const {
        return reinterpret_cast<const char*>(this) + sizeof(RowStoreMainEntry);
    }

    char* data() {
        return const_cast<char*>(const_cast<const RowStoreMainEntry*>(this)->data());
    }
};

template <typename T>
class RowStoreRecordImpl {
public:
    RowStoreRecordImpl(T entry)
            : mEntry(entry),
              mNewest(mEntry->newest.load()) {
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
        return mEntry->versionData()[0];
    }

    template <typename Fun>
    int get(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot, Fun fun, bool isNewest) const;

    bool needsCleaning(uint64_t minVersion) const;

    void collect(uint64_t minVersion, uint64_t highestVersion, std::vector<RecordHolder>& elements) const;

protected:
    T mEntry;
    uintptr_t mNewest;
};

template <typename T>
template <typename Fun>
int RowStoreRecordImpl<T>::get(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot, Fun fun,
        bool isNewest) const {
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

        auto offsets = mEntry->offsetData();
        auto size = offsets[i + 1] - offsets[i];
        if (size == 0) {
            return (isNewest ? error::not_found : error::not_in_snapshot);
        }

        auto dest = fun(size, versions[i], isNewest);
        memcpy(dest, reinterpret_cast<const char*>(mEntry) + offsets[i], size);
        return 0;
    }
    return (isNewest ? error::not_found : error::not_in_snapshot);
}

extern template class RowStoreRecordImpl<const RowStoreMainEntry*>;

class ConstRowStoreRecord : public RowStoreRecordImpl<const RowStoreMainEntry*> {
    using Base = RowStoreRecordImpl<const RowStoreMainEntry*>;
public:
    using Base::Base;

    ConstRowStoreRecord(const void* entry, const RowStoreContext& /* context */)
            : Base(reinterpret_cast<const RowStoreMainEntry*>(entry)) {
    }
};

extern template class RowStoreRecordImpl<RowStoreMainEntry*>;

class RowStoreRecord : public RowStoreRecordImpl<RowStoreMainEntry*> {
    using Base = RowStoreRecordImpl<RowStoreMainEntry*>;
public:
    using Base::Base;

    RowStoreRecord(void* entry, const RowStoreContext& /* context */)
            : Base(reinterpret_cast<RowStoreMainEntry*>(entry)) {
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
