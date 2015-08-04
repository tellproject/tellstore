/**
 * This file contains the parts of Record.cpp which are specific to the row store format.
 *
 * The memory layout of a row-store MV-DMRecord is:
 *    -1 byte: Record::Type
 *    -3 bytes padding
 *    -4 bytes integer storing the number of versions
 *    -8 bytes: key
 *
 *    - A pointer to the newest version
 *    - An array of version numbers
 *    - An array of size number_of_versions + 1 of 4 byte integers
 *      to store the offsets to
 *      the data for each version - this offset will be absolute.
 *      If the offset is euqal to the next offset, it means that the
 *      tuple was deleted at this version. The last offsets points to
 *      the byte after the record.
 *      If an offset is negative, its absolute value is still the
 *      size of the value, but the tuple is marked as reverted, it
 *      will be deleted at the next GC phase
 *    - A 4 byte padding if there are an even number of versions
 *
 *  - The data (if not delete)
 */

namespace impl {

template<class T>
class MVRecordBase {
protected:
    T mData;
public:
    using Type = typename DMRecordImplBase<T>::Type;
    MVRecordBase(T data) : mData(data) {}
    T getNewest() const {
        // The pointer format is like the following:
        // If (ptr % 2) -> this is a link, we need to
        //      follow this version.
        // else This is just a normal version - but it might be
        //      a MVRecord
        //
        // This const_cast is a hack - we might fix it
        // later
        char* data = const_cast<char*>(mData);
        auto ptr = reinterpret_cast<std::atomic<uint64_t>*>(data + 16);
        auto p = ptr->load();
        while (ptr->load() % 2) {
            // we need to follow this pointer
            ptr = reinterpret_cast<std::atomic<uint64_t>*>(p - 1);
            p = ptr->load();
        }
        return reinterpret_cast<char*>(p);
    }


    T dataPtr() {
        auto nV = getNumberOfVersions();
        auto offs = offsets();
        return mData + std::abs(offs[nV]);
    }

    bool isValidDataRecord() const {
        // a MVRecord is valid if at least one of the records
        // is valid
        auto nV = getNumberOfVersions();
        auto offs = offsets();
        for (decltype(nV) i = 0; i < nV; ++i) {
            if (offs[i] > 0) return true;
        }
        return false;
    }

    void revert(uint64_t version) {
        auto newest = getNewest();
        if (newest) {
            CDMRecord rec(newest);
            rec.revert(version);
            return;
        }
        auto nV = getNumberOfVersions();
        auto offs = offsets();
        LOG_ASSERT(versions()[nV-1] == version, "Can only revert newest version");
        const_cast<int32_t&>(offs[nV - 1]) *= -1;
    }

    bool casNewest(const char* expected, const char* desired) const {
        char* dataPtr = const_cast<char*>(mData);
        auto ptr = reinterpret_cast<std::atomic<uint64_t>*>(dataPtr + 16);
        auto p = ptr->load();
        while (ptr->load() % 2) {
            // we need to follow this pointer
            ptr = reinterpret_cast<std::atomic<uint64_t>*>(p - 1);
            p = ptr->load();
        }
        uint64_t exp = reinterpret_cast<const uint64_t>(expected);
        uint64_t des = reinterpret_cast<const uint64_t>(desired);
        if (p != exp) return false;
        return ptr->compare_exchange_strong(exp, des);
    }

    int32_t getNumberOfVersions() const {
        return *reinterpret_cast<const int32_t*>(mData + 4);
    }

    const uint64_t* versions() const {
        return reinterpret_cast<const uint64_t*>(mData + 24);
    }

    const int32_t* offsets() const {
        auto nVersions = getNumberOfVersions();
        size_t off = 24 + 8*nVersions;
        return reinterpret_cast<const int32_t*>(mData +off);
    }

    uint64_t size() const {
        auto off = offsets();
        auto v = getNumberOfVersions();
        return off[v];
    }

    bool needsCleaning(uint64_t lowestActiveVersion, InsertMap& insertMap) const {
        if (getNewest()) return true;
        auto offs = offsets();
        auto nV = getNumberOfVersions();
        // check whether there were some reverts
        for (decltype(nV) i = 0; i < nV; ++i) {
            if (offs[i] < 0) return true;
        }
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
            if (nV < lowestActiveVersion) return true;
            // otherwise we need to keep it, but it could be
            // that there was an insert
            CDMRecord rec(mData);
            return insertMap.count(rec.key());
        }
        return false;
    }

    const char* data(const commitmanager::SnapshotDescriptor& snapshot,
                     size_t& size,
                     uint64_t& version,
                     bool& isNewest,
                     bool& isValid,
                     bool* wasDeleted) const {
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
            if (off[idx] < 0) continue;
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
        if (std::abs(off[idx]) != std::abs(off[idx + 1])) {
            //TODO: question: if we are here, then size can never be 0, right?!
            size = size_t(std::abs(off[idx + 1]) - std::abs(off[idx]));
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
        auto nV = getNumberOfVersions();
        auto offs = offsets();
        isValid = true;
        for (decltype(nV) i = 0; i < nV; ++i) {
            if(offs[i] > 0)
                return Type::MULTI_VERSION_RECORD;
        }
        isValid = false;
        return Type::MULTI_VERSION_RECORD;
    }

    void collect(impl::VersionMap&, bool&, bool&) const {
        LOG_ASSERT(false, "should never call collect on MVRecord");
        std::cerr << "Fatal error!" << std::endl;
        std::terminate();
    }

    uint64_t copyAndCompact(
            uint64_t lowestActiveVersion,
            InsertMap& insertMap,
            char* dest,
            uint64_t maxSize,
            bool& success) const;
};

template<class T>
struct MVRecord : MVRecordBase<T> {
    MVRecord(T data) : MVRecordBase<T>(data) {}
};

template<>
struct MVRecord<char*> : GeneralUpdates<MVRecordBase<char*>> {
    MVRecord(char* data) : GeneralUpdates<MVRecordBase<char*>>(data) {}
    void writeVersion(uint64_t) {
        LOG_ERROR("You are not supposed to call this on a MVRecord");
        std::terminate();
    }
    void writePrevious(const char*) {
        LOG_ERROR("You are not supposed to call this on a MVRecord");
        std::terminate();
    }
    void writeData(size_t, const char*) {
        LOG_ERROR("You are not supposed to call this on a MVRecord");
        std::terminate();
    }

    uint64_t* versions() {
        return reinterpret_cast<uint64_t*>(mData + 24);
    }

    int32_t* offsets() {
        auto nVersions = getNumberOfVersions();
        size_t off = 24 + 8*nVersions;
        return reinterpret_cast<int32_t*>(mData +off);
    }

    char* dataPtr() {
        auto nVersions = getNumberOfVersions();
        size_t off = 24 + 8*nVersions + 4*(nVersions + 1);
        off += nVersions % 2 == 0 ? 4 : 0;
        return mData + off;
    }

    bool update(char* next,
                bool& isValid,
                const commitmanager::SnapshotDescriptor& snapshot) {
        auto newest = getNewest();
        if (newest) {
            DMRecord rec(newest);
            bool res = rec.update(next, isValid, snapshot);
            if (!res && isValid) return false;
            if (isValid) {
                if (rec.type() == MVRecord::Type::MULTI_VERSION_RECORD) return res;
                return casNewest(newest, next);
            }
        }
        auto versionIdx = getNumberOfVersions() - 1;
        auto v = versions();
        auto offs = offsets();
        for (; offs[versionIdx] < 0; --versionIdx) {
        }
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

template<class T>
uint64_t MVRecordBase<T>::copyAndCompact(
        uint64_t lowestActiveVersion,
        InsertMap& insertMap,
        char* dest,
        uint64_t maxSize,
        bool& success) const
{
        uint64_t offset = DMRecord::spaceOverhead(DMRecord::Type::MULTI_VERSION_RECORD);
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
            if (offs[i] > 0 && lowestActiveVersion <= v[i]) {
                if (i > newestValidVersionIdx)
                    newestValidVersionIdx = i;
                versions.insert(std::make_pair(v[i],
                            impl::VersionHolder {
                                mData + offs[i],
                                RecordType::MULTI_VERSION_RECORD,
                                size_t(std::abs(offs[i+1])) - offs[i],
                                nullptr}));
            }
        }
        if (versions.empty() && newest == nullptr) {
            if (newestValidVersionIdx < 0) {
                // All versions in this set are invalid, therefore, we could delete
                // the record right away. But there might be another insert with the
                // same key
            } else if (std::abs(offs[newestValidVersionIdx]) == std::abs(offs[newestValidVersionIdx - 1])) {
                // this tuple got deleted
            } else {
                // All records are older than the lowest active version
                // we need to make sure, that there is at least
                // one version in the record
                versions.insert(std::make_pair(v[newestValidVersionIdx],
                            impl::VersionHolder {
                            mData + offs[newestValidVersionIdx],
                            RecordType::MULTI_VERSION_RECORD,
                            size_t(std::abs(offs[newestValidVersionIdx+1]) - offs[newestValidVersionIdx]),
                            nullptr}));
            }
        }
        // now we need to collect all versions which are in the update and insert log
        CDMRecord myRec(mData);
        auto key = myRec.key();
        auto iter = insertMap.find(key);
        const char* current = newest;
        if (current == nullptr && iter != insertMap.end()
                && (versions.empty() || versions.rbegin()->second.size == 0)) {
            // there are inserts that need to be processed
            current = iter->second.front();
            iter->second.pop_front();
        }
        bool newestIsDelete = versions.empty() || versions.rbegin()->second.size == 0;
        while (current) {
            CDMRecord rec(current);
            bool allVersionsInvalid;
            rec.collect(versions, newestIsDelete, allVersionsInvalid);
            if (newestIsDelete || allVersionsInvalid) {
                if (iter != insertMap.end()) {
                    if (iter->second.empty()) {
                        insertMap.erase(iter);
                        break;
                    }
                    current = iter->second.front();
                    iter->second.pop_front();
                }
            } else {
                // in this case we do not need to check further for
                // inserts
                break;
            }
        }
        if (versions.empty()) {
            // there are no inserts with that key and the tuple
            // got either deleted or has no valid inserts. Therefore
            // we can delete the whole MVRecord
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
        // now we need to check whether the tuple will fit in the available memory
        // first we calculate the size of all tuples:
        size_t tupleDataSize = 0;
        auto firstValidVersion = versions.lower_bound(lowestActiveVersion);
        if (firstValidVersion == versions.end()) {
            --firstValidVersion;
        }
        versions.erase(versions.begin(), firstValidVersion);
        auto newNumberOfVersions = versions.size();
        for (auto iter = versions.begin(); iter != versions.end(); ++iter) {
            LOG_ASSERT(reinterpret_cast<const uint64_t>(iter->second.record) % 8 == 0,
                    "Record needs to be 8 byte aligned");
            LOG_ASSERT(iter->second.size % 8 == 0, "The size of a record has to be a multiple of 8");
            tupleDataSize += iter->second.size;
            ++newNumberOfVersions;
        }
        auto newTotalSize = offset;
        newTotalSize += 8*newNumberOfVersions;
        newTotalSize += 4*(newNumberOfVersions + 1);
        newTotalSize += newNumberOfVersions % 2 == 0 ? 4 : 0;
        newTotalSize += tupleDataSize;
        if (newTotalSize >= maxSize) {
            success = false;
            return 0;
        }
        dest[0] = crossbow::to_underlying(DMRecord::Type::MULTI_VERSION_RECORD);
        // now we can write the new version
        MVRecord<char*> newRec(dest);
        newRec.writeKey(key);
        *reinterpret_cast<uint32_t*>(dest+ 4) = uint32_t(newNumberOfVersions);
        uint64_t* newVersions = newRec.versions();
        int32_t* newOffsets = newRec.offsets();
        newOffsets[0] = dest - newRec.dataPtr();
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
                newestIns = i->second.nextPtr;
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
