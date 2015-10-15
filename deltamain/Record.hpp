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

#include <crossbow/enum_underlying.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace tell {
namespace commitmanager {

class SnapshotDescriptor;

} // namespace commitmanager

namespace store {

class PageManager;

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

class alignas(8) InsertLogEntry {
public:
    InsertLogEntry(uint64_t key, uint64_t version)
            : mKey(key),
              mVersion(version),
              mNewest(0x0u) {
    }

    uint64_t key() const {
        return mKey;
    }

    uint64_t version() const {
        return mVersion;
    }

    uintptr_t newest() const {
        return mNewest.load();
    }

    bool newest(uintptr_t& expected, uintptr_t value) {
        return mNewest.compare_exchange_strong(expected, value);
    }

    void invalidate() {
        mNewest.store(crossbow::to_underlying(NewestPointerTag::INVALID));
    }

    bool tryInvalidate(uintptr_t& expected) {
        return mNewest.compare_exchange_strong(expected, crossbow::to_underlying(NewestPointerTag::INVALID));
    }

    const char* data() const {
        return reinterpret_cast<const char*>(this) + sizeof(InsertLogEntry);
    }

    char* data() {
        return const_cast<char*>(const_cast<const InsertLogEntry*>(this)->data());
    }

private:
    const uint64_t mKey;
    const uint64_t mVersion;
    std::atomic<uintptr_t> mNewest;
};

template <typename T>
class InsertRecordImpl {
public:
    InsertRecordImpl(T record)
            : mRecord(record),
              mNewest(mRecord->newest()) {
    }

    T value() const {
        return mRecord;
    }

    uint64_t key() const {
        return mRecord->key();
    }

    bool valid() const {
        return (mNewest & crossbow::to_underlying(NewestPointerTag::INVALID)) == 0;
    }

    uintptr_t newest() const {
        return mNewest;
    }

    uint64_t baseVersion() const {
        return mRecord->version();
    }

    int get(uint64_t highestVersion, const commitmanager::SnapshotDescriptor& snapshot, size_t& size, const char*& data,
            uint64_t& version, bool& isNewest) const;

    void collect(uint64_t minVersion, uint64_t highestVersion, std::vector<RecordHolder>& elements) const;

protected:
    T mRecord;
    uintptr_t mNewest;
};

extern template class InsertRecordImpl<const InsertLogEntry*>;

class ConstInsertRecord : public InsertRecordImpl<const InsertLogEntry*> {
    using Base = InsertRecordImpl<const InsertLogEntry*>;
public:
    using Base::Base;

    ConstInsertRecord(const void* record)
            : Base(reinterpret_cast<const InsertLogEntry*>(record)) {
    }
};

extern template class InsertRecordImpl<InsertLogEntry*>;

class InsertRecord : public InsertRecordImpl<InsertLogEntry*> {
    using Base = InsertRecordImpl<InsertLogEntry*>;
public:
    using Base::Base;

    InsertRecord(void* record)
            : Base(reinterpret_cast<InsertLogEntry*>(record)) {
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

class alignas(8) UpdateLogEntry {
public:
    UpdateLogEntry(uint64_t key, uint64_t version, const UpdateLogEntry* previous)
            : mKey(key),
              mVersion(version),
              mPrevious(reinterpret_cast<uintptr_t>(previous)) {
    }

    uint64_t key() const {
        return mKey;
    }

    uint64_t version() const {
        return mVersion;
    }

    const UpdateLogEntry* previous() const {
        return reinterpret_cast<const UpdateLogEntry*>(mPrevious.load());
    }

    void invalidate() {
        mPrevious.store(crossbow::to_underlying(NewestPointerTag::INVALID));
    }

    const char* data() const {
        return reinterpret_cast<const char*>(this) + sizeof(UpdateLogEntry);
    }

    char* data() {
        return const_cast<char*>(const_cast<const UpdateLogEntry*>(this)->data());
    }

private:
    const uint64_t mKey;
    const uint64_t mVersion;
    std::atomic<uintptr_t> mPrevious;
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
