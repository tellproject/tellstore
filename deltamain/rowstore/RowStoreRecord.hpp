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

#include <crossbow/alignment.hpp>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace tell {
namespace commitmanager {

class SnapshotDescriptor;

} // namespace commitmanager

namespace store {
namespace deltamain {

class alignas(8) RowStoreMainEntry {
public:
    static uint32_t serializedHeaderSize(uint64_t versionCount) {
        // Size of header
        auto size = static_cast<uint32_t>(sizeof(RowStoreMainEntry));

        // Size of version array
        size += versionCount * sizeof(uint64_t);

        // Size of offset array
        size += (versionCount + 1) * sizeof(uint32_t);

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

    static RowStoreMainEntry* serialize(void* ptr, const RowStoreMainEntry* oldRecord, uint32_t oldSize);

    RowStoreMainEntry(uint64_t key, uint64_t versionCount)
            : mKey(key),
              mVersionCount(versionCount),
              mNewest(0x0u) {
    }

    uint64_t key() const {
        return mKey;
    }

    uint64_t numberOfVersions() const {
        return mVersionCount;
    }

    uintptr_t newest() const {
        return mNewest.load();
    }

    void newest(uintptr_t value) {
        return mNewest.store(value);
    }

    bool newest(uintptr_t& expected, uintptr_t value) {
        return mNewest.compare_exchange_strong(expected, value);
    }

    bool tryInvalidate(uintptr_t& expected) {
        return mNewest.compare_exchange_strong(expected, crossbow::to_underlying(NewestPointerTag::INVALID));
    }

    uint64_t newestVersion() const {
        return versionData()[0];
    }

    uint32_t size() const {
        return offsetData()[mVersionCount];
    }

    const uint64_t* versionData() const {
        return reinterpret_cast<const uint64_t*>(data());
    }

    uint64_t* versionData() {
        return const_cast<uint64_t*>(const_cast<const RowStoreMainEntry*>(this)->versionData());
    }

    const uint32_t* offsetData() const {
        return reinterpret_cast<const uint32_t*>(data() + mVersionCount * sizeof(uint64_t));
    }

    uint32_t* offsetData() {
        return const_cast<uint32_t*>(const_cast<const RowStoreMainEntry*>(this)->offsetData());
    }

private:
    const char* data() const {
        return reinterpret_cast<const char*>(this) + sizeof(RowStoreMainEntry);
    }

    char* data() {
        return const_cast<char*>(const_cast<const RowStoreMainEntry*>(this)->data());
    }

    const uint64_t mKey;
    const uint64_t mVersionCount;
    std::atomic<uintptr_t> mNewest;
};

template <typename T>
class RowStoreRecordImpl {
public:
    RowStoreRecordImpl(T record)
            : mRecord(record),
              mNewest(mRecord->newest()) {
    }

    uint64_t key() const {
        return mRecord->key();
    }

    bool valid() const {
        return (mNewest & crossbow::to_underlying(NewestPointerTag::INVALID)) == 0;
    }

    T value() const {
        return mRecord;
    }

    uintptr_t newest() const {
        return mNewest;
    }

    uint64_t baseVersion() const {
        return mRecord->newestVersion();
    }

    int get(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot, size_t& size, const char*& data,
            uint64_t& version, bool& isNewest) const;

    bool needsCleaning(uint64_t minVersion) const;

    void collect(uint64_t minVersion, uint64_t highestVersion, std::vector<RecordHolder>& elements) const;

protected:
    T mRecord;
    uintptr_t mNewest;
};

extern template class RowStoreRecordImpl<const RowStoreMainEntry*>;

class ConstRowStoreRecord : public RowStoreRecordImpl<const RowStoreMainEntry*> {
    using Base = RowStoreRecordImpl<const RowStoreMainEntry*>;
public:
    using Base::Base;

    ConstRowStoreRecord(const void* record)
            : Base(reinterpret_cast<const RowStoreMainEntry*>(record)) {
    }
};

extern template class RowStoreRecordImpl<RowStoreMainEntry*>;

class RowStoreRecord : public RowStoreRecordImpl<RowStoreMainEntry*> {
    using Base = RowStoreRecordImpl<RowStoreMainEntry*>;
public:
    using Base::Base;

    RowStoreRecord(void* record)
            : Base(reinterpret_cast<RowStoreMainEntry*>(record)) {
    }

    bool tryUpdate(uintptr_t value) {
        return mRecord->newest(mNewest, value);
    }

    bool tryInvalidate() {
        return mRecord->tryInvalidate(mNewest);
    }

    int canUpdate(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot, RecordType type) const;

    int canRevert(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot, bool& needsRevert) const;
};

} // namespace deltamain
} // namespace store
} // namespace tell
