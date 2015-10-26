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
 * The memory layout of a column-map MV-DMRecord depends on the memory layout of a
 * column map page which is layed out the following way:
 *
 * - count: uint32 to store the number of records that are actually stored in
 *   this page. We also define:
 *   count^: count rounded up to the next multiple of 2. This helps with proper
 *   8-byte alignment of values.
 * - 4-byte padding
 * - key-version column: an array of size count of 16-byte values in format:
 *   |key (8 byte)|version (8 byte)|
 * - newest-pointers: an array of size count of 8-byte pointers to newest
 *   versions of records in the logs (as their is only one newest ptr per MV
 *   record and not per record version, only the first newestptr of every MV
 *   record is valid)
 * - null-bitmatrix: a bitmatrix of size capacity x (|Columns|+7)/8 bytes
 * - var-size-meta-data column: an array of size count^ of signed 4-byte values
 *   indicating the total size of all var-sized values of each record (rounded up
 *   to the next multiple of 4. This is used to allocate enough space for a
 *   record on a get request.
 *   MOREOVER: We set this size to zero to denote a version of a deleted tuple
 *   and to a negative number if the tuple is marked as reverted and will be
 *   deleted at the next GC phase. In that case, the absolute denotes the size.
 * - fixed-sized data columns: for each column there is an array of size
 *   count^ x value-size (4 or 8 bytes, as defined in schema)
 * - var-sized data columns: for each colum there is an array of
 *   count  x 8 bytes in format:
 *   |4-byte-offset from page start into var-sized heap|4-byte prefix of value|
 * - var-sized heap: values referred from var-sized columns in the format
 *   |4-byte size (including the size field)|value|
 *
 * Pointers into a page (e.g. from log) point to the first key/version entry in
 * the key-version column, but have the second lowest bit set to 1 (in order to
 * make clear it is a columnMap-MV record). This bit has to be unset (by subtracting
 * 2) in order to get the correct address.
 *
 * MV records are stored as single records in such a way that they are clustered
 * together and ordered by version DESC (which facilitates get-operations).
 */

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
