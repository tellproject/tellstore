#include "Record.hpp"

#include <util/SnapshotDescriptor.hpp>
#include <util/Log.hpp>
#include <util/Logging.hpp>

#include <memory.h>
#include <map>

namespace tell {
namespace store {
namespace deltamain {

namespace {

template<class T>
struct GeneralUpdates : public T {
    GeneralUpdates(char* data) : T(data) {}
    void writeKey(uint64_t key) {
        memcpy(this->mData + 8, &key, sizeof(key));
    }
};

template<class T>
struct LogUpdates : public GeneralUpdates<T> {
public:
    LogUpdates(char* data) : GeneralUpdates<T>(data) {}
    void writeVersion(uint64_t version) {
        memcpy(this->mData + 16, &version, sizeof(version));
    }
    void writePrevious(const char* prev) {
        memcpy(this->mData + 24, &prev, sizeof(prev));
    }
    void writeData(size_t size, const char* data) {
        memcpy(this->mData + this->dataOffset(), data, size);
    }
};

template<class T>
class LogOp {
protected:
    T mData;
public:
    LogOp(T data) : mData(data) {}

    RecordType type() const {
        return from_underlying<RecordType>(*mData);
    }

    uint64_t version() const {
        return *reinterpret_cast<const uint64_t*>(mData+ 16);
    }

    T getPrevious() const {
        return *reinterpret_cast<const T*>(mData + 24);
    }

    uint64_t size() const {
        auto en = LogEntry::entryFromData(mData);
        return uint64_t(en->size());
    }

    uint64_t needsCleaning(uint64_t, InsertMap&) const {
        LOG_ERROR("needsCleaning does not make sense on Log operations");
        std::terminate();
    }

    bool isValidDataRecord() const {
        return mData[1] == 0;
    }

    T dataPtr()
    {
        return mData + DMRecord::spaceOverhead(type());
    }

};

template<class T>
class LogInsOrUpBase : public LogOp<T> {
public:
    LogInsOrUpBase(T data) : LogOp<T>(data) {}
    T data_ptr() const {
        return this->mData + 40;
    }
};

template<class T>
class LogInsOrUp : public LogInsOrUpBase<T> {
public:
    LogInsOrUp(T data) : LogInsOrUpBase<T>(data) {}
};

template<>
class LogInsOrUp<char*> : public LogInsOrUpBase<char*> {
public:
    LogInsOrUp(char* data) : LogInsOrUpBase<char*>(data) {}
};

template<class T>
class LogInsertBase : public LogInsOrUp<T> {
public:
    using Type = typename DMRecordImplBase<T>::Type;
    LogInsertBase(T data) : LogInsOrUp<T>(data) {}
    size_t dataOffset() const {
        return 40;
    }

    // insert log operations only have a newest pointer
    // but no previous one 
    T getNewest() const {
        // The pointer format is like the following:
        // If (ptr % 2) -> this is a link, we need to
        //      follow this version.
        // else This is just a normal version - but it might be
        //      a MVRecord
        //
        // This const_cast is a hack - we might fix it
        // later
        char* data = const_cast<char*>(this->mData);
        auto ptr = reinterpret_cast<std::atomic<uint64_t>*>(data + 24);
        auto p = ptr->load();
        while (ptr->load() % 2) {
            // we need to follow this pointer
            ptr = reinterpret_cast<std::atomic<uint64_t>*>(p - 1);
            p = ptr->load();
        }
        return reinterpret_cast<char*>(p);
    }

    void revert(uint64_t version) {
        if (this->version() != version) {
            auto newest = getNewest();
            LOG_ASSERT(newest != nullptr && this->version() > version, "Version to revert not found");
            CDMRecord rec(newest);
            rec.revert(version);
            return;
        }
        const_cast<char&>(this->mData[1]) = true;
    }

    bool casNewest(const char* expected, const char* desired) const {
        char* dataPtr = const_cast<char*>(this->mData);
        auto ptr = reinterpret_cast<std::atomic<uint64_t>*>(dataPtr + 24);
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

    const char* data(const SnapshotDescriptor& snapshot,
                     size_t& size,
                     bool& isNewest,
                     bool& isValid,
                     bool* wasDeleted) const {
        if (!this->isValidDataRecord()) {
            isValid = false;
            return nullptr;
        }
        auto v = this->version();
        auto next = this->getNewest();
        if (next) {
            // In this case we always have to check the newer versions first.
            // It could be, that there are newer versions which are in the
            // snapshots read set even if the current version is not in the
            // read set (there can be wholes in the read set).
            bool b = false;
            DMRecordImplBase<T> rec(next);
            auto res = rec.data(snapshot, size, isNewest, isValid, &b);
            if (isValid && (b || res)) {
                if (wasDeleted) *wasDeleted = b;
                return res;
            }
            // we only set isNewest to false if the newest pointer
            // references invalid data
            isNewest = isValid;
        }
        isValid = true;
        if (snapshot.inReadSet(v)) {
            if (wasDeleted) *wasDeleted = false;
            if (next == nullptr) isNewest = true;
            auto entry = LogEntry::entryFromData(this->mData);
            size = size_t(entry->size()) - DMRecord::spaceOverhead(this->type());
            return this->data_ptr();
        }
        isNewest = false;
        size = 0;
        return nullptr;
    }

    Type typeOfNewestVersion(bool& isValid) const {
        if (this->isValidDataRecord()) {
            // in that case we know that there are no updates
            isValid = false;
            return RecordType::LOG_INSERT;
        }
        auto next = this->getNewest();
        if (next) {
            DMRecordImplBase<T> rec(next);
            bool newestIsValid;
            auto res = rec.typeOfNewestVersion(newestIsValid);
            if (newestIsValid) return res;
        }
        return Type::LOG_INSERT;
    }

    uint64_t copyAndCompact(uint64_t, InsertMap&, char*, uint64_t,bool&) const {
        // TODO: Implement!
        LOG_ERROR("Not yet implemented");
        std::terminate();
    }
};

template<class T>
class LogInsert : public LogInsertBase<T> {
public:
    LogInsert(T data) : LogInsertBase<T>(data) {}
};

template<>
class LogInsert<char*> : public LogUpdates<LogInsertBase<char*>> {
public:
    LogInsert(char* data) : LogUpdates<LogInsertBase<char*>>(data) {}

    bool update(char* next, bool& isValid, const SnapshotDescriptor& snapshot) {
        if (!this->isValidDataRecord()) {
            isValid = false;
            return false;
        }
        isValid = true;
        if (!snapshot.inReadSet(version())) return false;
        auto newest = getNewest();
        if (newest) {
            DMRecord rec(newest);
            bool newestIsValid;
            auto res = rec.update(next, newestIsValid, snapshot);
            if (newestIsValid && !res) return false;
        }
        return casNewest(newest, next);
    }
};

template<class T>
class LogUpdateBase : public LogInsOrUp<T> {
public:
    using Type = typename DMRecordImplBase<T>::Type;
    LogUpdateBase(T data) : LogInsOrUp<T>(data) {}

    size_t dataOffset() const {
        return 32;
    }

    void revert(uint64_t v) {
        LOG_ASSERT(v == this->version(), "Can only revert newest version");
        const_cast<char&>(this->mData[1]) = 1;
    }

    const char* data(const SnapshotDescriptor& snapshot,
                     size_t& size,
                     bool& isNewest,
                     bool& isValid,
                     bool* wasDeleted) const {
        if (!this->isValidDataRecord()) {
            isValid = false;
            return nullptr;
        }
        isValid = true;
        auto v = this->version();
        if (snapshot.inReadSet(v)) {
            if (wasDeleted) *wasDeleted = false;
            auto entry = LogEntry::entryFromData(this->mData);
            size = size_t(entry->size());
            return this->data_ptr();
        } else {
            auto prev = this->getPrevious();
            isNewest = false;
            if (prev) {
                DMRecordImplBase<T> rec(prev);
                return rec.data(snapshot, size, isNewest, isValid, wasDeleted);
            }
            if (wasDeleted) *wasDeleted = false;
            return nullptr;
        }
    }

    Type typeOfNewestVersion(bool& isValid) const {
        if (!this->isValidDataRecord()) {
            auto prev = this->getPrevious();
            if (prev == nullptr) {
                isValid = false;
                return RecordType::LOG_UPDATE;
            }
            CDMRecord rec(prev);
            return rec.typeOfNewestVersion(isValid);
        }
        isValid = true;
        return Type::LOG_UPDATE;
    }

    uint64_t copyAndCompact(uint64_t, InsertMap, char*, uint64_t,bool&) const {
        LOG_ERROR("copyAndCompact does not make sense on Log operations");
        std::terminate();
    }
};

template<class T>
class LogUpdate : public LogUpdateBase<T> {
public:
    LogUpdate(T data) : LogUpdateBase<T>(data) {}
};

template<>
class LogUpdate<char*> : public LogUpdates<LogUpdateBase<char*>> {
public:
    LogUpdate(char* data) : LogUpdates<LogUpdateBase<char*>>(data) {}

    bool update(char* data, bool& isValid, const SnapshotDescriptor& snapshot)
    {
        LOG_ASSERT(this->isValidDataRecord(), "Invalid log updates should not be reachable");
        isValid = true;
        if (!snapshot.inReadSet(version()))
            return false;
        DMRecord rec(data);
        rec.writePrevious(this->mData);
        return true;
    }
};

template<class T>
class LogDeleteBase : public LogOp<T> {
public:
    using Type = typename DMRecordImplBase<T>::Type;
    LogDeleteBase(T data) : LogOp<T>(data) {}

    size_t dataOffset() const {
        return 32;
    }

    const char* data(const SnapshotDescriptor& snapshot,
                     size_t& size,
                     bool& isNewest,
                     bool& isValid,
                     bool* wasDeleted) const {
        if (!this->isValidDataRecord()) {
            isValid = false;
            return nullptr;
        }
        isValid = true;
        auto v = this->version();
        if (snapshot.inReadSet(v)) {
            if (wasDeleted) *wasDeleted = true;
            size = 0;
            return nullptr;
        } else {
            auto prev = this->getPrevious();
            if (prev) {
                DMRecordImplBase<T> rec(prev);
                auto res = rec.data(snapshot, size, isNewest, isValid, wasDeleted);
                isNewest = false;
                return res;
            }
            // in this case, the tuple never existed for the running
            // transaction. Therefore it does not see the deletion
            if (wasDeleted) *wasDeleted = false;
            size = 0;
            return nullptr;
        }
    }

    void revert(uint64_t v) {
        LOG_ASSERT(v == this->version(), "Can only revert newest version");
        const_cast<char&>(this->mData[1]) = 1;
    }

    Type typeOfNewestVersion(bool& isValid) const {
        if (!this->isValidDataRecord()) {
            isValid = false;
            auto prev = this->getPrevious();
            if (prev) {
                CDMRecord rec(prev);
                return rec.typeOfNewestVersion(isValid);
            }
            return RecordType::LOG_DELETE;
        }
        isValid = true;
        return Type::LOG_DELETE;
    }

    uint64_t copyAndCompact(uint64_t, InsertMap&, char*, uint64_t,bool&) const {
        LOG_ERROR("copyAndCompact does not make sense on Log operations");
        std::terminate();
    }
};

template<class T>
class LogDelete : public LogDeleteBase<T> {
public:
    LogDelete(T data) : LogDeleteBase<T>(data) {}
};

template<>
class LogDelete<char*> : public LogUpdates<LogDeleteBase<char*>> {
public:
    LogDelete(char* data) : LogUpdates<LogDeleteBase<char*>>(data) {}

    bool update(char*, bool&, const SnapshotDescriptor&)
    {
        LOG_ASSERT(false, "Calling update on a deleted record is an invalid operation");
        // the client has to do an insert in this case!!
        return false;
    }
};

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
            // The last version got deleted - it could be that
            // there is an insert record
            CDMRecord rec(mData);
            return insertMap.count(rec.key());
        }
        return false;
    }

    const char* data(const SnapshotDescriptor& snapshot,
                     size_t& size,
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
            auto res = rec.data(snapshot, s, isNewest, isValid, &b);
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
                break;
            }
            isNewest = false;
        }
        if (idx < 0) {
            if (wasDeleted) *wasDeleted = false;
            return nullptr;
        }
        if (std::abs(off[idx]) != std::abs(off[idx + 1])) {
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
                const SnapshotDescriptor& snapshot) {
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
            if (offs[i] > 0 && lowestActiveVersion >= v[i]) {
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
                // All versions are invalid. This means that it even never got
                // inserted, therefore wo do not need to check anything else but
                // can return right away
                success = true;
                return 0;
            }
            if (std::abs(offs[newestValidVersionIdx]) == std::abs(offs[newestValidVersionIdx - 1])) {
                // this tuple got deleted 
                // but we need to check
                // if another transaction did reinsert it afterwards
                CDMRecord myRec(mData);
                auto iter = insertMap.find(myRec.key());
                if (iter != insertMap.end()) {
                    auto& vec = iter->second;
                    auto insLogEntry = vec.front();
                    if (vec.size() == 1) {
                        insertMap.erase(iter);
                    } else {
                        vec.pop_front();
                    }
                    CDMRecord insRec(insLogEntry);
                    return insRec.copyAndCompact(lowestActiveVersion, insertMap, dest, maxSize, success);
                }
                // This tuple got deleted and we can
                // safely discard it!
                success = true;
                return 0;
            }
            // we need to make sure, that there is at least
            // one version in the record
            versions.insert(std::make_pair(v[newestValidVersionIdx],
                        impl::VersionHolder {
                            mData + offs[newestValidVersionIdx],
                            RecordType::MULTI_VERSION_RECORD,
                            size_t(std::abs(offs[newestValidVersionIdx+1]) - offs[newestValidVersionIdx]),
                            nullptr}));
        }
        // now we need to collect all versions which are in the update and insert log
        CDMRecord myRec(mData);
        auto key = myRec.key();
        auto iter = insertMap.find(key);
        const char* current = newest;
        if (current == nullptr && iter != insertMap.end()
                && offs[nV] == offs[nV - 1]) {
            // there are inserts that need to be processed
            current = *(iter->second.begin());
            iter->second.pop_front();
        }
        bool newestIsDelete;
        while (current) {
            CDMRecord rec(current);
            rec.collect(versions, newestIsDelete);
            if (newestIsDelete) {
                if (iter != insertMap.end()) {
                    if (iter->second.empty()) {
                        insertMap.erase(iter);
                        break;
                    }
                    current = iter->second.front();
                    iter->second.pop_front();
                }
            }
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
        if (newTotalSize >= maxSize) return 0;
        dest[0] = to_underlying(DMRecord::Type::MULTI_VERSION_RECORD);
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
        return newTotalSize;
}

} // namespace {}

#define DISPATCH_METHOD(T, methodName,  ...) switch(this->type()) {\
case Type::LOG_INSERT:\
    {\
        LogInsert<T> rec(this->mData);\
        return rec.methodName(__VA_ARGS__);\
    }\
case Type::LOG_UPDATE:\
    {\
        LogUpdate<T> rec(this->mData);\
        return rec.methodName(__VA_ARGS__);\
    }\
case Type::LOG_DELETE:\
    {\
        LogDelete<T> rec(this->mData);\
        return rec.methodName(__VA_ARGS__);\
    }\
case Type::MULTI_VERSION_RECORD:\
    {\
        MVRecord<T> rec(this->mData);\
        return rec.methodName(__VA_ARGS__);\
    }\
}

#define DISPATCH_METHODT(methodName,  ...) DISPATCH_METHOD(T, methodName, __VA_ARGS__)
#define DISPATCH_METHOD_NCONST(methodName, ...) DISPATCH_METHOD(char*, methodName, __VA_ARGS__)

template<class T>
const char* DMRecordImplBase<T>::data(const SnapshotDescriptor& snapshot,
                                  size_t& size,
                                  bool& isNewest,
                                  bool& isValid,
                                  bool *wasDeleted /* = nullptr */) const {
    // we have to execute this at a readable version
    isNewest = true;
    DISPATCH_METHODT(data, snapshot, size, isNewest, isValid, wasDeleted);
    return nullptr;
}

template<class T>
auto DMRecordImplBase<T>::typeOfNewestVersion(bool& isValid) const -> Type {
    DISPATCH_METHODT(typeOfNewestVersion, isValid);
}

template<class T>
size_t DMRecordImplBase<T>::spaceOverhead(Type t) {
    switch(t) {
    case Type::LOG_INSERT:
    case Type::LOG_UPDATE:
    case Type::LOG_DELETE:
        return 32;
    case Type::MULTI_VERSION_RECORD:
        return 24;
    }
}

template<class T>
bool DMRecordImplBase<T>::needsCleaning(uint64_t lowestActiveVersion, InsertMap& insertMap) const {
    DISPATCH_METHODT(needsCleaning, lowestActiveVersion, insertMap);
}

template<class T>
uint64_t DMRecordImplBase<T>::size() const {
    DISPATCH_METHODT(size);
}

template<class T>
T DMRecordImplBase<T>::dataPtr()
{
    DISPATCH_METHODT(dataPtr);
}

template<class T>
uint64_t DMRecordImplBase<T>::copyAndCompact(
        uint64_t lowestActiveVersion,
        InsertMap& insertMap,
        char* newLocation,
        uint64_t maxSize,
        bool& success) const
{
    DISPATCH_METHODT(copyAndCompact, lowestActiveVersion, insertMap, newLocation, maxSize, success);
}

template<class T>
void DMRecordImplBase<T>::revert(uint64_t version) {
    DISPATCH_METHODT(revert, version);
}

template<class T>
bool DMRecordImplBase<T>::isValidDataRecord() const {
    DISPATCH_METHODT(isValidDataRecord);
}

void DMRecordImpl<char*>::writeKey(uint64_t key) {
    DISPATCH_METHOD_NCONST(writeKey, key);
}

void DMRecordImpl<char*>::writeVersion(uint64_t version) {
    DISPATCH_METHOD_NCONST(writeVersion, version);
}

void DMRecordImpl<char*>::writePrevious(const char* prev) {
    DISPATCH_METHOD_NCONST(writePrevious, prev);
}

void DMRecordImpl<char*>::writeData(size_t size, const char* data) {
    DISPATCH_METHOD_NCONST(writeData, size, data);
}

bool DMRecordImpl<char*>::update(char* next,
                                 bool& isValid,
                                 const SnapshotDescriptor& snapshot) {
    DISPATCH_METHOD_NCONST(update, next, isValid, snapshot);
} 

template class DMRecordImplBase<const char*>;
template class DMRecordImplBase<char*>;
template class DMRecordImpl<const char*>;
template class DMRecordImpl<char*>;

} // namespace deltamain
} // namespace store
} // namespace tell
