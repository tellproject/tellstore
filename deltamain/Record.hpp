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

#include <util/Log.hpp>

#include <tellstore/ErrorCode.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/enum_underlying.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace tell {
namespace store {
namespace deltamain {

struct RecordHolder {
    RecordHolder(uint64_t v, const char* d, size_t s)
            : version(v),
              data(d),
              size(s) {
    }

    /// Version of the element
    uint64_t version;

    /// Pointer to the data directly (no key or version information)
    const char* data;

    /// Size of the data
    size_t size;
};

enum RecordType : uint32_t {
    DATA = 0x1u,
    DELETE,
    REVERT,
};

enum NewestPointerTag : uintptr_t {
    UPDATE = 0x0u,
    MAIN = 0x1u,
    INVALID = (0x1u << 1),
};

inline void* newestMainRecord(uintptr_t newest) {
    if ((newest & crossbow::to_underlying(NewestPointerTag::MAIN)) != 0x0u) {
        return reinterpret_cast<void*>(newest & ~crossbow::to_underlying(NewestPointerTag::MAIN));
    }
    return nullptr;
}

struct alignas(8) InsertLogEntry {
    InsertLogEntry(uint64_t k, uint64_t v)
            : key(k),
              version(v),
              newest(0x0u) {
    }

    const char* data() const {
        return reinterpret_cast<const char*>(this) + sizeof(InsertLogEntry);
    }

    char* data() {
        return const_cast<char*>(const_cast<const InsertLogEntry*>(this)->data());
    }

    const uint64_t key;
    const uint64_t version;
    std::atomic<uintptr_t> newest;
};

template <typename T>
class InsertRecordImpl {
public:
    InsertRecordImpl(T entry)
            : mEntry(entry),
              mNewest(mEntry->newest.load()) {
    }

    T value() const {
        return mEntry;
    }

    uint64_t key() const {
        return mEntry->key;
    }

    bool valid() const {
        return (mNewest & crossbow::to_underlying(NewestPointerTag::INVALID)) == 0;
    }

    uintptr_t newest() const {
        return mNewest;
    }

    uint64_t baseVersion() const {
        return mEntry->version;
    }

    template <typename Fun>
    int get(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot, Fun fun, bool isNewest) const;

    void collect(uint64_t minVersion, uint64_t highestVersion, std::vector<RecordHolder>& elements) const;

protected:
    T mEntry;
    uintptr_t mNewest;
};

template <typename T>
template <typename Fun>
int InsertRecordImpl<T>::get(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot, Fun fun,
        bool isNewest) const {
    // Check if the element was already overwritten by an element in the update log
    if (mEntry->version >= highestVersion) {
        return (isNewest ? error::not_found : error::not_in_snapshot);
    }
    if (!snapshot.inReadSet(mEntry->version)) {
        return error::not_in_snapshot;
    }

    auto logEntry = LogEntry::entryFromData(reinterpret_cast<const char*>(mEntry));
    auto size = logEntry->size() - sizeof(InsertLogEntry);
    auto dest = fun(size, mEntry->version, isNewest);
    memcpy(dest, mEntry->data(), size);
    return 0;
}

extern template class InsertRecordImpl<const InsertLogEntry*>;

class ConstInsertRecord : public InsertRecordImpl<const InsertLogEntry*> {
    using Base = InsertRecordImpl<const InsertLogEntry*>;
public:
    using Base::Base;

    template <typename Context>
    ConstInsertRecord(const void* record, const Context& /* context */)
            : Base(reinterpret_cast<const InsertLogEntry*>(record)) {
    }
};

extern template class InsertRecordImpl<InsertLogEntry*>;

class InsertRecord : public InsertRecordImpl<InsertLogEntry*> {
    using Base = InsertRecordImpl<InsertLogEntry*>;
public:
    using Base::Base;

    template <typename Context>
    InsertRecord(void* record, const Context& /* context */)
            : Base(reinterpret_cast<InsertLogEntry*>(record)) {
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

struct alignas(8) UpdateLogEntry {
    UpdateLogEntry(uint64_t k, uint64_t v, const UpdateLogEntry* p)
            : key(k),
              version(v),
              previous(reinterpret_cast<uintptr_t>(p)) {
    }

    const char* data() const {
        return reinterpret_cast<const char*>(this) + sizeof(UpdateLogEntry);
    }

    char* data() {
        return const_cast<char*>(const_cast<const UpdateLogEntry*>(this)->data());
    }

    const uint64_t key;
    const uint64_t version;
    std::atomic<uintptr_t> previous;
};

class UpdateRecordIterator {
public:
    UpdateRecordIterator(const UpdateLogEntry* record, uint64_t baseVersion);

    bool done() const {
        return mCurrent == nullptr;
    }

    void next();

    uint64_t lowestVersion() const {
        return mLowestVersion;
    }

    /**
     * @brief The current element
     */
    const UpdateLogEntry* value() const {
        return mCurrent;
    }

    const UpdateLogEntry& operator*() const {
        return *operator->();
    }

    const UpdateLogEntry* operator->() const {
        return mCurrent;
    }

private:
    const UpdateLogEntry* mCurrent;
    uint64_t mBaseVersion;
    uint64_t mLowestVersion;
};

} // namespace deltamain
} // namespace store
} // namespace tell
