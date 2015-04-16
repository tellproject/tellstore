#include "Record.hpp"

#include <util/SnapshotDescriptor.hpp>
#include <util/Log.hpp>
#include <util/Logging.hpp>

#include <memory.h>

namespace tell {
namespace store {

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

    uint64_t version() const {
        return *reinterpret_cast<const uint64_t*>(mData+ 16);
    }

    T getPrevious() const {
        return *reinterpret_cast<const T*>(mData + 24);
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

    T getNext() const {
        return *reinterpret_cast<const T*>(this->mData + 32);
    }

    const char* data(const SnapshotDescriptor& snapshot,
                     size_t& size,
                     bool& isNewest,
                     bool* wasDeleted) const {
        auto v = this->version();
        auto next = this->getNext();
        if (next) {
            // In this case we always have to check the newer versions first.
            // It could be, that there are newer versions which are in the
            // snapshots read set even if the current version is not in the
            // read set (there can be wholes in the read set).
            bool b = false;
            DMRecordImplBase<T> rec(next);
            auto res = rec.data(snapshot, size, isNewest, &b);
            if (b || res) {
                if (wasDeleted) *wasDeleted = b;
                return res;
            }
            isNewest = false;
        }
        if (snapshot.inReadSet(v)) {
            // we do not need to check for older versions in this case.
            if (wasDeleted) *wasDeleted = false;
            if (next == nullptr) isNewest = true;
            auto entry = LogEntry::entryFromData(this->mData);
            size = size_t(entry->size());
            return this->data_ptr();
        }
        isNewest = false;
        auto prev = this->getPrevious();
        if (prev) {
            DMRecordImplBase<T> rec(prev);
            return rec.data(snapshot, size, isNewest, wasDeleted);
        }
        if (wasDeleted) *wasDeleted = false;
        size = 0;
        return nullptr;
    }

    Type typeOfNewestVersion() const {
        auto next = this->getNext();
        if (next) {
            DMRecordImplBase<T> rec(next);
            return rec.typeOfNewestVersion();
        }
        return Type::LOG_INSERT;
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
    void writeNextPtr(const char* ptr) {
        memcpy(this->mData + 32, &ptr, sizeof(ptr));
    }

    bool casNextPtr(char* expected, char* desired) {
        std::atomic<char*>& next = *reinterpret_cast<std::atomic<char*>*>(this->mData + 32);
        while (true) {
            if (next.compare_exchange_strong(expected, desired)) return true;
            if (next.load() != expected) return false;
        }
    }

    bool update(char* next, const SnapshotDescriptor& snapshot) {
        if (!snapshot.inReadSet(version())) return false;
        auto newest = getNext();
        if (newest) {
            DMRecord rec(newest);
            auto res = rec.update(next, snapshot);
            if (!res) return false;
        }
        return casNextPtr(newest, next);
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

    const char* data(const SnapshotDescriptor& snapshot,
                     size_t& size,
                     bool& isNewest,
                     bool* wasDeleted) const {
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
                return rec.data(snapshot, size, isNewest, wasDeleted);
            }
            if (wasDeleted) *wasDeleted = false;
            return nullptr;
        }
    }

    Type typeOfNewestVersion() const {
        return Type::LOG_UPDATE;
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
    void writeNextPtr(const char*) {
        LOG_ERROR("Call not allowed here");
        std::terminate();
    }

    bool update(char* data, const SnapshotDescriptor& snapshot)
    {
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
                     bool* wasDeleted) const {
        auto v = this->version();
        if (snapshot.inReadSet(v)) {
            if (wasDeleted) *wasDeleted = true;
            size = 0;
            return nullptr;
        } else {
            auto prev = this->getPrevious();
            if (prev) {
                DMRecordImplBase<T> rec(prev);
                auto res = rec.data(snapshot, size, isNewest, wasDeleted);
                isNewest = false;
                return res;
            }
            // in this case, the tuple never existed for the running
            // transaction. Therefore it does not see the deletion
            if (wasDeleted) *wasDeleted = false;
            return nullptr;
        }
    }

    Type typeOfNewestVersion() const {
        return Type::LOG_DELETE;
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
    void writeNextPtr(const char*) {
        LOG_ERROR("Call not allowed here");
        std::terminate();
    }

    bool update(char* data, const SnapshotDescriptor& snapshot)
    {
        if (!snapshot.inReadSet(version()))
            return false;
        DMRecord rec(data);
        rec.writePrevious(this->mData);
        return true;
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
        return mData + 16;
    }
    
    uint32_t getNumberOfVersions() const {
        return *reinterpret_cast<const uint32_t*>(mData + 4);
    }

    const uint64_t* versions() const {
        return reinterpret_cast<const uint64_t*>(mData + 24);
    }

    const uint32_t* offsets() const {
        auto nVersions = getNumberOfVersions();
        size_t off = 24 + 8*nVersions;
        return reinterpret_cast<const uint32_t*>(mData +off);
    }

    const char* data(const SnapshotDescriptor& snapshot,
                     size_t& size,
                     bool& isNewest,
                     bool* wasDeleted) const {
        auto numVersions = getNumberOfVersions();
        auto v = versions();
        auto newest = getNewest();
        if (newest) {
            DMRecordImplBase<T> rec(newest);
            bool b;
            size_t s;
            auto res = rec.data(snapshot, s, isNewest, &b);
            if (b || res) {
                if (wasDeleted) *wasDeleted = b;
                size = s;
                return res;
            }
            isNewest = false;
        }
        int idx = numVersions - 1;
        for (; idx >=0; --idx) {
            if (snapshot.inReadSet(v[idx])) {
                break;
            }
            isNewest = false;
        }
        if (idx < 0) {
            if (wasDeleted) *wasDeleted = false;
            return nullptr;
        }
        auto off = offsets();
        if (off[idx]) {
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

    Type typeOfNewestVersion() const {
        auto newest = getNewest();
        if (newest) {
            DMRecordImplBase<T> rec(newest);
            return rec.typeOfNewestVersion();
        }
        return Type::MULTI_VERSION_RECORD;
    }
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
    void writeNextPtr(const char*) {
        LOG_ERROR("Call not allowed here");
        std::terminate();
    }

    bool casNewest(const char*, const char*) {
        // TODO: Implement
        return false;
    }

    bool update(char* next,
                const SnapshotDescriptor& snapshot) {
        auto newest = getNewest();
        if (newest) {
            DMRecord rec(newest);
            bool res = rec.update(next, snapshot);
            if (!res) return false;
            return casNewest(newest, next);
        }
        auto nVersions = getNumberOfVersions();
        auto v = versions();
        if (snapshot.inReadSet(v[nVersions - 1]))
            return false;
        DMRecord nextRec(next);
        nextRec.writePrevious(this->mData);
        return casNewest(newest, next);
    }
};

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
                                  bool *wasDeleted /* = nullptr */) const {
    // we have to execute this at a readable version
    isNewest = true;
    DISPATCH_METHODT(data, snapshot, size, isNewest, wasDeleted);
    return nullptr;
}

template<class T>
auto DMRecordImplBase<T>::typeOfNewestVersion() const -> Type {
    DISPATCH_METHODT(typeOfNewestVersion);
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
        return 24;
    }
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

void DMRecordImpl<char*>::writeNextPtr(const char* ptr) {
    DISPATCH_METHOD_NCONST(writeNextPtr, ptr);
}

bool DMRecordImpl<char*>::update(char* next,
                                 const SnapshotDescriptor& snapshot) {
    DISPATCH_METHOD_NCONST(update, next, snapshot);
} 

template class DMRecordImplBase<const char*>;
template class DMRecordImplBase<char*>;
template class DMRecordImpl<const char*>;
template class DMRecordImpl<char*>;

} // namespace store
} // namespace tell
