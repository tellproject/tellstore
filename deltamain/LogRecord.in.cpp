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
namespace tell {
namespace store {
namespace deltamain {
namespace impl {

/**
 *  The memory layout of a LOG-DMRecord is:
 *   - 1 byte: Record::Type
 *   - 1 byte with a boolean, indicating whether the entry
 *     got reverted
 *   - 6 bytes padding
 *   - 8 bytes: key
 *   - 8 bytes: version
 *   - 8 bytes: pointer to a previous version. this will
 *      always be set to null, if the previous version was
 *      not an update log entry. If the previous version
 *      was an insert log entry, the only way to reach the
 *      update is via the insert entry, if it was a multi
 *      version record, we can only reach it via the
 *      record entry itself. This is an important design
 *      decision: this way me make clear that we do not
 *      introduce cycles.
 *      If the log operation is an insert,
 *      this position holds the pointer to the newest version
 *
 *   - The data (if not delete)
 */

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
        return crossbow::from_underlying<RecordType>(*mData);
    }

    uint64_t version() const {
        return *reinterpret_cast<const uint64_t*>(mData + 16);
    }

    T getPrevious() const {
        return *reinterpret_cast<const T*>(mData + 24);
    }

    size_t recordSize() const {
        auto sz = size_t(size());
        sz -= DMRecord::spaceOverhead(type());
        return sz;
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

    T dataPtr() const
    {
        return mData + DMRecord::spaceOverhead(type());
    }

};

template<class T>
class LogInsOrUpBase : public LogOp<T> {
public:
    LogInsOrUpBase(T data) : LogOp<T>(data) {}
    T data_ptr() const {
        return this->mData + 32;
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
        return 32;
    }

    std::atomic<const char*>* getNewestAtomic() const {
        char* data = const_cast<char*>(this->mData);
        return reinterpret_cast<std::atomic<const char*>*>(data + 24);
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
        //TODO: check whether this loop does the correct thing... doesn't
        //this assume that the newestPtr is stored at the beginning of a
        //log record (which is actually not the case)?
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

    const char* data(const commitmanager::SnapshotDescriptor& snapshot,
                     size_t& size,
                     uint64_t& version,
                     bool& isNewest,
                     bool& isValid,
                     bool* wasDeleted,
                     const Table *table,
                     bool copyData
    ) const {
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
            auto res = rec.data(snapshot, size, version, isNewest, isValid, &b);
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
            version = v;
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

    void collect(impl::VersionMap& versionMap, bool& newestIsDelete, bool& allVersionsInvalid) const {
        allVersionsInvalid = true;
        if (this->isValidDataRecord()) {
            versionMap.insert(std::make_pair(this->version(),
                    impl::VersionHolder{
                        this->dataPtr(),
                        RecordType::LOG_INSERT,
                        this->recordSize(),
                        getNewestAtomic() }));
            newestIsDelete = false;
            allVersionsInvalid = false;
        }
        auto newest = getNewest();
        if (newest) {
            CDMRecord rec(newest);
            rec.collect(versionMap, newestIsDelete, allVersionsInvalid);
        }
    }

    uint64_t copyAndCompact(uint64_t, InsertMap&, char*, uint64_t,bool&) const {
        LOG_ERROR("Do not call this on a log insert!");
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

    bool update(char* next, bool& isValid, const commitmanager::SnapshotDescriptor& snapshot,const Table *table = nullptr) {
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

    const char* data(const commitmanager::SnapshotDescriptor& snapshot,
                     size_t& size,
                     uint64_t& version,
                     bool& isNewest,
                     bool& isValid,
                     bool* wasDeleted,
                     const Table *table,
                     bool copyData
    ) const {
        if (!this->isValidDataRecord()) {
            isValid = false;
            return nullptr;
        }
        isValid = true;
        auto v = this->version();
        if (snapshot.inReadSet(v)) {
            version = v;
            if (wasDeleted) *wasDeleted = false;
            auto entry = LogEntry::entryFromData(this->mData);
            size = size_t(entry->size());
            return this->data_ptr();
        } else {
            auto prev = this->getPrevious();
            isNewest = false;
            if (prev) {
                DMRecordImplBase<T> rec(prev);
                return rec.data(snapshot, size, version, isNewest, isValid, wasDeleted);
            }
            if (wasDeleted) *wasDeleted = false;
            return nullptr;
        }
    }

    void collect(impl::VersionMap& versionMap, bool& newestIsDelete, bool& allVersionsInvalid) const
    {
        allVersionsInvalid = allVersionsInvalid || this->isValidDataRecord();
        auto prev = this->getPrevious();
        if (prev) {
            CDMRecord rec(prev);
            rec.collect(versionMap, newestIsDelete, allVersionsInvalid);
        }
        if (this->isValidDataRecord()) {
            newestIsDelete = false;
            versionMap.emplace(this->version(),
                    impl::VersionHolder{
                        this->dataPtr(),
                        RecordType::LOG_UPDATE,
                        this->recordSize(),
                        nullptr
                    });
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

    bool update(char* data, bool& isValid, const commitmanager::SnapshotDescriptor& snapshot, const Table *table = nullptr)
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

    const char* data(const commitmanager::SnapshotDescriptor& snapshot,
                     size_t& size,
                     uint64_t& version,
                     bool& isNewest,
                     bool& isValid,
                     bool* wasDeleted,
                     const Table *table = nullptr,
                     bool copyData = false
    ) const {
        if (!this->isValidDataRecord()) {
            isValid = false;
            return nullptr;
        }
        isValid = true;
        auto v = this->version();
        if (snapshot.inReadSet(v)) {
            version = v;
            if (wasDeleted) *wasDeleted = true;
            size = 0;
            return nullptr;
        } else {
            auto prev = this->getPrevious();
            if (prev) {
                DMRecordImplBase<T> rec(prev);
                auto res = rec.data(snapshot, size, version, isNewest, isValid, wasDeleted);
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

    void collect(impl::VersionMap& versionMap, bool& newestIsDelete, bool& allVersionsInvalid) const
    {
        allVersionsInvalid = allVersionsInvalid || this->isValidDataRecord();
        auto prev = this->getPrevious();
        if (prev) {
            CDMRecord rec(prev);
            rec.collect(versionMap, newestIsDelete, allVersionsInvalid);
        }
        if (this->isValidDataRecord()) {
            newestIsDelete = true;
            versionMap.emplace(this->version(),
                    impl::VersionHolder{
                        this->dataPtr(),
                        RecordType::LOG_DELETE,
                        this->recordSize(),
                        nullptr
                    });
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

    bool update(char*, bool&, const commitmanager::SnapshotDescriptor&, const Table *table = nullptr)
    {
        LOG_ASSERT(false, "Calling update on a deleted record is an invalid operation");
        // the client has to do an insert in this case!!
        return false;
    }
};

} // namespace impl
} // namespace deltamain
} // namespace store
} // namespace tell
