#include "Record.hpp"

#include <util/Log.hpp>

#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/logger.hpp>

#include <memory.h>
#include <map>

#include "Table.hpp"

namespace tell {
namespace store {
namespace deltamain {

namespace impl {

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
        return *reinterpret_cast<const uint64_t*>(mData+ 16);
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


/**
 * For MV record, we only give definitions while the storage-specific
 * implementations are either in RowStoreRecord.cpp or ColumMapRecord.cpp
 * depending on which storage layout was chosen.
 */

template<class T>
class MVRecordBase {
protected:
    T mData;
public:
    using Type = typename DMRecordImplBase<T>::Type;
    MVRecordBase(T data) : mData(data) {}

    /**
     * TODO: For now, this function has default parameter in order for the
     * VersionIterator to compile. Once we really want to make it useful
     * we have to force the caller to give non-null arguments!
     */
    T getNewest(const Table *table = nullptr, const uint32_t index = 0, const char * basePtr = nullptr, const uint32_t capacity = 0) const;
    T dataPtr();
    bool isValidDataRecord() const;
    void revert(uint64_t version);
    bool casNewest(const char* expected, const char* desired,const Table *table) const;
    int32_t getNumberOfVersions() const;
    const uint64_t* versions() const;
    const int32_t* offsets() const;
    uint64_t size() const;
    bool needsCleaning(uint64_t lowestActiveVersion, InsertMap& insertMap) const;
    const char* data(const commitmanager::SnapshotDescriptor& snapshot,
                     size_t& size,
                     uint64_t& version,
                     bool& isNewest,
                     bool& isValid,
                     bool* wasDeleted,
                     const Table *table,
                     bool copyData
    ) const;
    Type typeOfNewestVersion(bool& isValid) const;
    void collect(impl::VersionMap&, bool&, bool&) const;
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

    uint64_t* versions();
    int32_t* offsets();
    char* dataPtr();
    bool update(char* next,
                bool& isValid,
                const commitmanager::SnapshotDescriptor& snapshot,
                const Table *table = nullptr
    );
};

template<class T>
void MVRecordBase<T>::collect(impl::VersionMap&, bool&, bool&) const {
    LOG_ASSERT(false, "should never call collect on MVRecord");
    std::cerr << "Fatal error!" << std::endl;
    std::terminate();
}

} // namespace impl

/**
* INSERT STORAGE-LAYOUT-SPECIFIC IMPLEMENTATIONS OF MV RECORD
*/

#if defined USE_ROW_STORE
#include "RowStoreRecord.cpp"
#elif defined USE_COLUMN_MAP
#include "ColumnMapRecord.cpp"
#else
#error "Unknown storage layout"
#endif

template<class T>
DMRecordImplBase<T>::VersionIterator::VersionIterator(const Record* record, const char* current)
    : record(record)
    , current(current)
{
    currEntry.mRecord = record;
    // go to the first valid entry
    while (current) {
        CDMRecord rec(current);
        if (rec.type() == RecordType::MULTI_VERSION_RECORD) {
            impl::MVRecord<const char*> mvRec(current);
            auto numV = mvRec.getNumberOfVersions();
            auto offs = mvRec.offsets();
            for (decltype(numV) i = 0; i < numV; ++i) {
                if (offs[i] > 0) {
                    idx = i;
                    goto END;
                }
            }
            // this MVRecord only contains invalid data, but this should
            // never happen
            LOG_ERROR("You must never get an iterator on an invalid (reverted) record");
            std::terminate();
        } else if (rec.type() == RecordType::LOG_INSERT) {
            return;
        } else {
            LOG_ERROR("A Version iterator must always start either at in insert or on a MVRecord");
            std::terminate();
        }
    }
END:
    initRes();
}

template<class T>
auto DMRecordImplBase<T>::VersionIterator::operator++() -> VersionIterator&
{
    while (current != nullptr) {
        CDMRecord rec(current);
        if (rec.type() == RecordType::MULTI_VERSION_RECORD) {
            impl::MVRecord<const char*> mvRec(current);
            auto numV = mvRec.getNumberOfVersions();
            auto offs = mvRec.offsets();
            ++idx;
            while (numV > idx && offs[idx] < 0);
            if (numV == idx) {
                // we reachted the end of the MVRecord
                current = mvRec.getNewest();
            } else {
                // we are done
                goto END;
            }
        } else if (rec.type() == RecordType::LOG_INSERT) {
            impl::LogInsert<const char*> ins(current);
            if (!ins.isValidDataRecord()) {
                current = nullptr;
            } else {
                goto END;
            }
        } else if (rec.type() == RecordType::LOG_DELETE) {
            current = nullptr;
        } else if (rec.type() == RecordType::LOG_UPDATE) {
            impl::LogUpdate<const char*> upd(current);
            if (upd.isValidDataRecord()) {
                goto END;
            } else {
                current = upd.getPrevious();
            }
        }
    }
END:
    initRes();
    return *this;
}

template<class T>
void DMRecordImplBase<T>::VersionIterator::initRes()
{
    if (current == nullptr) return;
    CDMRecord rec(current);
    if (rec.type() == RecordType::MULTI_VERSION_RECORD) {
        impl::MVRecord<const char*> mvRec(current);
        auto nV = mvRec.getNumberOfVersions();
        auto versions = mvRec.versions();
        auto offs = mvRec.offsets();
        currEntry.mData = current + offs[idx];
        currEntry.mSize = offs[idx + 1] - offs[idx];
        currEntry.mValidFrom = versions[idx];
        if (idx == nV - 1) {
            // we need to check the next pointer in order to be able
            // to get the validTo property
            auto n = mvRec.getNewest();
            while (n != nullptr) {
                impl::LogOp<const char*> rc(n);
                if (rc.isValidDataRecord()) break;
                n = rc.getPrevious();
            }
            if (n == nullptr) {
                currEntry.mValidTo = std::numeric_limits<uint64_t>::max();
            } else {
                impl::LogOp<const char*> r(n);
                currEntry.mValidTo = r.version();
            }
        } else {
            currEntry.mValidTo = versions[idx + 1];
        }
    } else if (rec.type() == RecordType::LOG_INSERT) {
        impl::LogInsert<const char*> insRec(current);
        currEntry.mData = insRec.dataPtr();
        currEntry.mSize = insRec.recordSize();
        currEntry.mValidFrom = insRec.version();
        auto n = insRec.getNewest();
        while (n != nullptr) {
            impl::LogOp<const char*> rc(n);
            if (rc.isValidDataRecord()) break;
            n = rc.getPrevious();
        }
        if (n == nullptr) {
            currEntry.mValidTo = std::numeric_limits<uint64_t>::max();
        } else {
            impl::LogOp<const char*> r(n);
            currEntry.mValidTo = r.version();
        }
    } else if (rec.type() == RecordType::LOG_UPDATE) {
        impl::LogUpdate<const char*> up(current);
        currEntry.mData = up.dataPtr();
        currEntry.mSize = up.recordSize();
        currEntry.mValidTo = currEntry.mValidFrom;
        currEntry.mValidFrom = up.version();
    }
}

template<class T>
const typename DMRecordImplBase<T>::VersionIterator::IteratorEntry& DMRecordImplBase<T>::VersionIterator::operator* () const
{
    return currEntry;
}

template<class T>
const typename DMRecordImplBase<T>::VersionIterator::IteratorEntry* DMRecordImplBase<T>::VersionIterator::operator-> () const
{
    return &currEntry;
}

#define DISPATCH_METHOD(T, methodName,  ...) switch(this->type()) {\
case Type::LOG_INSERT:\
    {\
        impl::LogInsert<T> rec(this->mData);\
        return rec.methodName(__VA_ARGS__);\
    }\
case Type::LOG_UPDATE:\
    {\
        impl::LogUpdate<T> rec(this->mData);\
        return rec.methodName(__VA_ARGS__);\
    }\
case Type::LOG_DELETE:\
    {\
        impl::LogDelete<T> rec(this->mData);\
        return rec.methodName(__VA_ARGS__);\
    }\
case Type::MULTI_VERSION_RECORD:\
    {\
        impl::MVRecord<T> rec(this->mData);\
        return rec.methodName(__VA_ARGS__);\
    }\
}

#define DISPATCH_METHODT(methodName,  ...) DISPATCH_METHOD(T, methodName, __VA_ARGS__)
#define DISPATCH_METHOD_NCONST(methodName, ...) DISPATCH_METHOD(char*, methodName, __VA_ARGS__)

template<class T>
auto DMRecordImplBase<T>::getVersionIterator(const Record* record) const -> VersionIterator
{
    return VersionIterator(record, this->mData);
}

template<class T>
const char* DMRecordImplBase<T>::data(const commitmanager::SnapshotDescriptor& snapshot,
                                  size_t& size,
                                  uint64_t& version,
                                  bool& isNewest,
                                  bool& isValid,
                                  bool *wasDeleted /* = nullptr */
#if defined USE_COLUMN_MAP
                                  ,
                                  const Table *table, /* = nullptr */
                                  bool copyData /* = true */
#endif
            ) const {
    isNewest = true;
#if defined USE_ROW_STORE
    DISPATCH_METHODT(data, snapshot, size, version, isNewest, isValid, wasDeleted);
#elif defined USE_COLUMN_MAP
    DISPATCH_METHODT(data, snapshot, size, version, isNewest, isValid, wasDeleted, table, copyData);
#else
#error "Unknown storage layout"
#endif
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
        return 40;
    case Type::LOG_UPDATE:
    case Type::LOG_DELETE:
        return 32;
    case Type::MULTI_VERSION_RECORD:
#if defined USE_ROW_STORE
        return 24;
#elif defined USE_COLUMN_MAP
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
        std::terminate();
#else
#error "Unknown storage layout"
#endif
    }
}

template<class T>
bool DMRecordImplBase<T>::needsCleaning(uint64_t lowestActiveVersion, InsertMap& insertMap) const {
    DISPATCH_METHODT(needsCleaning, lowestActiveVersion, insertMap);
}

template<class T>
void DMRecordImplBase<T>::collect(impl::VersionMap& versions, bool& newestIsDelete, bool& allVersionsInvalid) const
{
    DISPATCH_METHODT(collect, versions, newestIsDelete, allVersionsInvalid);
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
                                 const commitmanager::SnapshotDescriptor& snapshot
#if defined USE_COLUMN_MAP
                                 ,
                                 const Table *table
#endif
) {
#if defined USE_COLUMN_MAP
    DISPATCH_METHOD_NCONST(update, next, isValid, snapshot, table);
#else
    DISPATCH_METHOD_NCONST(update, next, isValid, snapshot);
#endif
} 

template class DMRecordImplBase<const char*>;
template class DMRecordImplBase<char*>;
template class DMRecordImpl<const char*>;
template class DMRecordImpl<char*>;

} // namespace deltamain
} // namespace store
} // namespace tell
