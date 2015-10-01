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

#include <deltamain/LogRecord.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

namespace tell {
namespace store {
namespace deltamain {
namespace impl {

/**
 * The memory layout of a row-store MV-DMRecord is:
 *    -1 byte: Record::Type
 *    -3 bytes padding
 *    -4 bytes integer storing the number of versions
 *    -8 bytes: key
 *
 *    - An 8-byte pointer to the newest version
 *    - An array of version numbers
 *    - An array of size number_of_versions + 1 of 4 byte integers
 *      to store the offsets to the data for each version.
 *      If the offset is equal to the next offset, it means that the
 *      tuple was deleted at this version. The last offsets points to
 *      the byte after the record.
 *    - A 4 byte padding if there are an even number of versions
 *
 *    - The data (if not delete)
 */

template<class T>
class RowStoreMVRecordBase {
protected:
    T mData;
public:
    using Type = typename DMRecordImplBase<T>::Type;
    RowStoreMVRecordBase(T data) : mData(data) {}

    T getNewest() const {
        // The pointer format is like the following:
        // If (ptr % 2) -> this is a link, we need to
        //      follow this version.
        // else This is just a normal version - but it might be
        //      a RowStoreMVRecord
        //
        // This const_cast is a hack - we might fix it
        // later
        char* data = const_cast<char*>(mData);
        auto ptr = reinterpret_cast<std::atomic<uint64_t>*>(data + 16);
        auto p = ptr->load();
        //TODO: check whether this loop does the correct thing... doesn't
        //this assume that the newestPtr is stored at the beginning of a
        //log record (which is actually not the case)?
        while (p % 2) {
            // we need to follow this pointer
            ptr = reinterpret_cast<std::atomic<uint64_t>*>(p - 1);
            p = ptr->load();
        }
        return reinterpret_cast<T>(p);
    }


    T dataPtr() {
        auto nV = getNumberOfVersions();
        auto offs = offsets();
        return mData + offs[nV];
    }

    bool isValidDataRecord() const {
        // a RowStoreMVRecord is valid if at least one of the records
        // is valid
        auto nV = getNumberOfVersions();
        return nV > 0u;
    }

    void revert(uint64_t /* version */) {
        LOG_FATAL("Can not revert a MVRecord");
        std::terminate();
    }

    bool casNewest(const char* expected, const char* desired) const {
        char* dataPtr = const_cast<char*>(mData);
        auto ptr = reinterpret_cast<std::atomic<uint64_t>*>(dataPtr + 16);
        auto p = ptr->load();
        //TODO: check whether this loop does the correct thing... doesn't
        //this assume that the newestPtr is stored at the beginning of a
        //log record (which is actually not the case)?
        while (p % 2) {
            // we need to follow this pointer
            ptr = reinterpret_cast<std::atomic<uint64_t>*>(p - 1);
            p = ptr->load();
        }
        uint64_t exp = reinterpret_cast<const uint64_t>(expected);
        uint64_t des = reinterpret_cast<const uint64_t>(desired);
        if (p != exp) return false;
        return ptr->compare_exchange_strong(exp, des);
    }

    uint32_t getNumberOfVersions() const {
        return *reinterpret_cast<const uint32_t*>(mData + 4);
    }

    const uint64_t *versions() const {
        return reinterpret_cast<const uint64_t*>(mData + 24);
    }

    const uint32_t *offsets() const {
        auto nVersions = getNumberOfVersions();
        size_t off = 24 + 8*nVersions;
        return reinterpret_cast<const uint32_t*>(mData + off);
    }

    uint64_t size() const {
        auto off = offsets();
        auto v = getNumberOfVersions();
        return off[v];
    }

    bool needsCleaning(uint64_t lowestActiveVersion, const InsertMap& insertMap) const {
        if (getNewest()) return true;
        auto offs = offsets();
        auto nV = getNumberOfVersions();
        if (versions()[0] < lowestActiveVersion) {
            if (nV == 1) {
                // Check if this was a delition
                // in that case we will either have
                // to delete the record from the table,
                // or merge it if there was an update.
                // We do not need to test which case
                // is true here.
                return offs[0] == offs[1];
            }
            return true;
        }
        if (offs[nV] == offs[nV - 1]) {
            // The last version got deleted
            // If the newest version is smaller than the
            // lowest active version, we can delete the whole
            // entry.
            if (versions()[nV - 1] < lowestActiveVersion) return true;
            // otherwise we need to keep it, but it could be
            // that there was an insert
            CDMRecord rec(mData);
            return insertMap.count(rec.key());
        }
        return false;
    }

    const char *data(const commitmanager::SnapshotDescriptor& snapshot,
                     size_t& size,
                     uint64_t& version,
                     bool& isNewest,
                     bool& isValid,
                     bool* wasDeleted,
                     const Table *table,
                     bool copyData
    ) const {
        auto numVersions = getNumberOfVersions();
        auto v = versions();
        auto newest = getNewest();
        if (newest) {
            DMRecordImplBase<T> rec(newest);
            bool b;
            size_t s;
            auto res = rec.data(snapshot, s, version, isNewest, isValid, &b);
            if (isValid) {
                if (b || res) {
                    if (wasDeleted) *wasDeleted = b;
                    size = s;
                    return res;
                }
                isNewest = false;
            }
        }
        isValid = false;
        int idx = numVersions - 1;
        auto off = offsets();
        for (; idx >=0; --idx) {
            isValid = true;
            if (snapshot.inReadSet(v[idx])) {
                version = v[idx];
                break;
            }
            isNewest = false;
        }
        if (idx < 0) {
            if (wasDeleted) *wasDeleted = false;
            return nullptr;
        }
        if (off[idx] != off[idx + 1]) {
            //TODO: question: if we are here, then size can never be 0, right?!
            size = size_t(off[idx + 1] - off[idx]);
            if (wasDeleted) {
                // a tuple is deleted, if its size is 0
                *wasDeleted = size == 0;
            }
            return mData + off[idx];
        }
        if (wasDeleted) *wasDeleted = true;
        return nullptr;
    }

    Type typeOfNewestVersion(bool& isValid) const {
        auto newest = getNewest();
        if (newest) {
            DMRecordImplBase<T> rec(newest);
            auto res = rec.typeOfNewestVersion(isValid);
            if (isValid) return res;
        }
        isValid = true;
        return Type::MULTI_VERSION_RECORD;
    }

    uint64_t copyAndCompact(
            uint64_t lowestActiveVersion,
            const InsertMap& insertMap,
            char* dest,
            uint64_t maxSize,
            bool& success) const;

    void collect(impl::VersionMap&, bool&, bool&) const {
        LOG_ASSERT(false, "should never call collect on MVRecord");
        std::cerr << "Fatal error!" << std::endl;
        std::terminate();
    }
};

template<class T>
struct RowStoreMVRecord : RowStoreMVRecordBase<T> {
    RowStoreMVRecord(T data) : RowStoreMVRecordBase<T>(data) {}
};

template<>
struct RowStoreMVRecord<char*> : GeneralUpdates<RowStoreMVRecordBase<char*>> {
    RowStoreMVRecord(char* data) : GeneralUpdates<RowStoreMVRecordBase<char*>>(data) {}
    void writeVersion(uint64_t) {
        LOG_ERROR("You are not supposed to call this on MVRecord");
        std::terminate();
    }
    void writePrevious(const char*) {
        LOG_ERROR("You are not supposed to call this on MVRecord");
        std::terminate();
    }
    void writeData(size_t, const char*) {
        LOG_ERROR("You are not supposed to call this on MVRecord");
        std::terminate();
    }

    uint64_t* versions() {
        return reinterpret_cast<uint64_t*>(mData + 24);
    }

    uint32_t* offsets() {
        auto nVersions = getNumberOfVersions();
        size_t off = 24 + 8*nVersions;
        return reinterpret_cast<uint32_t*>(mData +off);
    }

    char* dataPtr() {
        auto nVersions = getNumberOfVersions();
        size_t off = 24 + 8*nVersions + 4*(nVersions + 1);
        off += nVersions % 2 == 0 ? 4 : 0;
        return mData + off;
    }

    bool update(char* next,
                bool& isValid,
                const commitmanager::SnapshotDescriptor& snapshot,
                const Table *table) {
        auto newest = getNewest();
        if (newest) {
            DMRecord rec(newest);
            bool res = rec.update(next, isValid, snapshot);
            if (!res && isValid) return false;
            if (isValid) {
                //TODO: question: why can this at all be an MVRecord?! GC?
                if (rec.type() == RowStoreMVRecord::Type::MULTI_VERSION_RECORD) return res;
                return casNewest(newest, next);
            }
        }
        auto versionIdx = getNumberOfVersions() - 1;
        auto v = versions();
        if (versionIdx < 0) {
            isValid = false;
            return false;
        }
        isValid = true;
        if (snapshot.inReadSet(v[versionIdx - 1]))
            return false;
        DMRecord nextRec(next);
        nextRec.writePrevious(this->mData);
        return casNewest(newest, next);
    }
};

template <class T>
uint64_t RowStoreMVRecordBase<T>::copyAndCompact(
        uint64_t lowestActiveVersion,
        const InsertMap& insertMap,
        char* dest,
        uint64_t maxSize,
        bool& success) const
{
        auto v = versions();
        auto offs = offsets();
        auto nV = getNumberOfVersions();
        if (!needsCleaning(lowestActiveVersion, insertMap)) {
            // just copy the record
            auto sz = size();
            if (sz > maxSize) {
                success = false;
                return 0;
            }
            success = true;
            memcpy(dest, mData, sz);
            return sz;
        }
        auto newest = getNewest();
        impl::VersionMap versions;
        int32_t newestValidVersionIdx = -1;
        for (decltype(nV) i = 0; i < nV; ++i) {
            if (lowestActiveVersion <= v[i]) {
                if (static_cast<int32_t>(i) > newestValidVersionIdx)
                    newestValidVersionIdx = i;
                versions.insert(std::make_pair(v[i],
                            impl::VersionHolder {
                                mData + offs[i],
                                RecordType::MULTI_VERSION_RECORD,
                                offs[i+1] - offs[i],
                                nullptr}));
            }
        }
        if (versions.empty() && newest == nullptr) {
            if (newestValidVersionIdx < 0) {
                // All versions in this set are invalid, therefore, we could delete
                // the record right away. But there might be another insert with the
                // same key
            } else if (offs[newestValidVersionIdx] == offs[newestValidVersionIdx - 1]) {
                // this tuple got deleted
            } else {
                // All records are older than the lowest active version
                // we need to make sure, that there is at least
                // one version in the record
                versions.insert(std::make_pair(v[newestValidVersionIdx],
                            impl::VersionHolder {
                            mData + offs[newestValidVersionIdx],
                            RecordType::MULTI_VERSION_RECORD,
                            offs[newestValidVersionIdx+1] - offs[newestValidVersionIdx],
                            nullptr}));
            }
        }
        // now we need to collect all versions which are in the update and insert log
        CDMRecord myRec(mData);
        auto key = myRec.key();
        const char* current = newest;
        bool newestIsDelete = versions.empty() || versions.rbegin()->second.size == 0;
        auto iter = insertMap.find(key);
        decltype(iter->second.begin()) insertsIter;
        decltype(iter->second.end()) insertsEnd;
        if (iter != insertMap.end()) {
            insertsIter = iter->second.begin();
            insertsEnd = iter->second.end();
        }
        if (current == nullptr && insertsIter != insertsEnd && newestIsDelete) {
            // there are inserts that need to be processed
            current = *insertsIter;
            ++insertsIter;
        }
        while (current) {
        //TODO: shouldn't newest somewhere get the value of the newestptr of the newest insert?! --> This way, we might loose updates!
            CDMRecord rec(current);
            bool allVersionsInvalid;
            rec.collect(versions, newestIsDelete, allVersionsInvalid);
            if ((newestIsDelete || allVersionsInvalid) && insertsIter != insertsEnd) {
                current = *insertsIter;
                ++insertsIter;
            } else {
                // in this case we do not need to check further for
                // inserts
                break;
            }
        }
        if (versions.empty()) {
            // there are no inserts with that key and the tuple
            // got either deleted or has no valid inserts. Therefore
            // we can delete the whole RowStoreMVRecord
            success = true;
            return 0;
        }
        // this should be a very rare corner case, but it could happen, that there
        // are still no versions in the read set and the newest version is already
        // a delete operation
        // Note that we compare here for greater equal and not just greater: if
        // the version got deleted anyway, it will not be seen by any active transaction
        // with the version equal to the lowest active version.
        if (newestIsDelete && lowestActiveVersion >= versions.rend()->first) {
            // we are done
            success = true;
            return 0;
        }
        auto firstValidVersion = versions.lower_bound(lowestActiveVersion);
        if (firstValidVersion == versions.end()) {
            --firstValidVersion;
        }
        versions.erase(versions.begin(), firstValidVersion);

        // now we need to check whether the tuple will fit in the available memory
        // first we calculate the size of all tuples:
        size_t tupleDataSize = 0;
        auto newNumberOfVersions = versions.size();
        for (auto iter = versions.begin(); iter != versions.end(); ++iter) {
            LOG_ASSERT(reinterpret_cast<const uint64_t>(iter->second.record) % 8 == 0,
                    "Record needs to be 8 byte aligned");
            LOG_ASSERT(iter->second.size % 8 == 0, "The size of a record has to be a multiple of 8");
            tupleDataSize += iter->second.size;
        }
        auto newHeaderSize = DMRecord::spaceOverhead(DMRecord::Type::MULTI_VERSION_RECORD);
        newHeaderSize += 8*newNumberOfVersions;
        newHeaderSize += 4*(newNumberOfVersions + 1);
        newHeaderSize += newNumberOfVersions % 2 == 0 ? 4 : 0;
        auto newTotalSize = newHeaderSize;
        newTotalSize += tupleDataSize;
        if (newTotalSize >= maxSize) {
            success = false;
            return 0;
        }
        dest[0] = crossbow::to_underlying(DMRecord::Type::MULTI_VERSION_RECORD);
        // now we can write the new version
        RowStoreMVRecord<char*> newRec(dest);
        newRec.writeKey(key);
        *reinterpret_cast<uint32_t*>(dest + 4) = uint32_t(newNumberOfVersions);
        auto newVersions = newRec.versions();
        auto newOffsets = newRec.offsets();
        newOffsets[0] = newHeaderSize;
        newOffsets[newNumberOfVersions] = newTotalSize;
        uint32_t offsetCounter = 0;
        std::atomic<const char*>* newestIns = nullptr;
        for (auto i = versions.begin(); i != versions.end(); ++i) {
            newVersions[offsetCounter] = i->first;
            newOffsets[offsetCounter + 1] = newOffsets[offsetCounter] + i->second.size;
            memcpy(dest + newOffsets[offsetCounter], i->second.record, i->second.size);
            // only if the newest inserted version comes from a insert log entry we want
            // to indirect this pointer
            newestIns = nullptr;
            if (i->second.type == DMRecord::Type::LOG_INSERT) {
                newestIns = i->second.newestPtrLocation;
            }
            ++offsetCounter;
        }
        // The new record is written, now we just have to write the new-pointer
        auto newNewestPtr = newestIns;
        if (newNewestPtr == nullptr)
            newNewestPtr = reinterpret_cast<std::atomic<const char*>*>(dest + 16);
        newNewestPtr->store(nullptr);
        while (!casNewest(newest, dest + 1)) {
            newest = getNewest();
            newNewestPtr->store(newest); // this newest version is also valid after GC finished
        }
        success = true;
        return newTotalSize;
}

} // namespace impl
} // namespace deltamain
} // namespace store
} // namespace tell
