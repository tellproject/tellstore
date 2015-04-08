#include "Record.hpp"

#include <util/SnapshotDescriptor.hpp>
#include <util/Log.hpp>

namespace tell {
namespace store {

namespace {

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
        return mData + 24;
    }

    T getNext() const {
        return mData + 32;
    }
};

template<class T>
class LogInsOrUp : public LogOp<T> {
public:
    LogInsOrUp(T data) : LogOp<T>(data) {}
    T data_ptr() const {
        return this->mData + 40;
    }
};

template<class T>
class LogInsert : public LogInsOrUp<T> {
public:
    LogInsert(T data) : LogInsOrUp<T>(data) {}
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
            DMRecordImpl<T> rec(next);
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
            auto entry = LogEntry::entryFromData(this->data_ptr());
            size = size_t(entry->size());
            return this->data_ptr();
        }
        isNewest = false;
        auto prev = this->getPrevious();
        if (prev) {
            DMRecordImpl<T> rec(prev);
            return rec.data(snapshot, size, isNewest, wasDeleted);
        }
        if (wasDeleted) *wasDeleted = false;
        size = 0;
        return nullptr;
    }

};

template<class T>
class LogUpdate : public LogInsOrUp<T> {
public:
    LogUpdate(T data) : LogInsOrUp<T>(data) {}
    const char* data(const SnapshotDescriptor& snapshot,
                     size_t& size,
                     bool& isNewest,
                     bool* wasDeleted) const {
        auto v = this->version();
        if (snapshot.inReadSet(v)) {
            if (wasDeleted) *wasDeleted = false;
            auto entry = LogEntry::entryFromData(this->data_ptr());
            size = size_t(entry->size());
            return this->data_ptr();
        } else {
            auto prev = this->getPrevious();
            isNewest = false;
            if (prev) {
                DMRecordImpl<T> rec(prev);
                return rec.data(snapshot, size, isNewest, wasDeleted);
            }
            if (wasDeleted) *wasDeleted = false;
            return nullptr;
        }
    }
};

template<class T>
class LogDelete : public LogOp<T> {
public:
    LogDelete(T data) : LogOp<T>(data) {}
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
                DMRecordImpl<T> rec(prev);
                auto res = rec.data(snapshot, isNewest, wasDeleted);
                isNewest = false;
                return res;
            }
            // in this case, the tuple never existed for the running
            // transaction. Therefore it does not see the deletion
            if (wasDeleted) *wasDeleted = false;
            return nullptr;
        }
    }
};

template<class T>
class MVRecord {
    T mData;
public:
    MVRecord(T data) : mData(data) {}
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
            DMRecordImpl<T> rec(newest);
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
            if (wasDeleted) *wasDeleted = false;
            size = size_t(off[idx + 1] - off[idx]);
            return mData + off[idx];
        }
        if (wasDeleted) *wasDeleted = true;
        return nullptr;
    }
};

} // namespace {}

template<class T>
const char* DMRecordImpl<T>::data(const SnapshotDescriptor& snapshot,
                                  size_t& size,
                                  bool& isNewest,
                                  bool *wasDeleted /* = nullptr */) const {
    // we have to execute this at a readable version
    isNewest = true;
    switch (this->type()) {
    case Type::LOG_INSERT:
        {
            LogInsert<T> ins(this->mData);
            return ins.data(snapshot, size, isNewest, wasDeleted);
        }
    case Type::LOG_UPDATE:
        {
            LogUpdate<T> up(this->mData);
            return up.data(snapshot, size, isNewest, wasDeleted);
        }
    case Type::LOG_DELETE:
        {
            LogDelete<T> del(this->mData);
            return del.data(snapshot, size, isNewest, wasDeleted);
        }
    case Type::MULTI_VERSION_RECORD:
        {
            MVRecord<T> rec(this->mData);
            return rec.data(snapshot, size, isNewest, wasDeleted);
        }
    }
    return nullptr;
}

template class DMRecordImpl<const char*>;
template class DMRecordImpl<char*>;

} // namespace store
} // namespace tell
