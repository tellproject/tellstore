#include "Record.hpp"
#include "LogRecord.hpp"
#include "rowstore/RowStoreRecord.hpp"
#include "colstore/ColumnMapRecord.hpp"

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

#if defined USE_ROW_STORE
template< class T>
using MVRecord = RowStoreMVRecord<T>;
#elif defined USE_COLUMN_MAP
template< class T>
using MVRecord = ColMapMVRecord<T>;
#else
#error "Unknown storage layout"
#endif

} // namespace impl

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
default:\
    {\
        LOG_ERROR("Unknown record type");\
        std::terminate();\
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
                                  bool *wasDeleted /* = nullptr */,
                                  const Table *table, /* = nullptr */
                                  bool copyData /* = true */
) const {
    isNewest = true;
    DISPATCH_METHODT(data, snapshot, size, version, isNewest, isValid, wasDeleted, table, copyData);
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
        LOG_ASSERT(false, "You are not supposed to call this on a columMap MVRecord");
        return 0;
#else
#error "Unknown storage layout"
#endif
    default:
        LOG_ASSERT(false, "Unknown record type");
        return 0;
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
                                 const commitmanager::SnapshotDescriptor& snapshot,
                                 const Table *table
) {
    DISPATCH_METHOD_NCONST(update, next, isValid, snapshot, table);
} 

template class DMRecordImplBase<const char*>;
template class DMRecordImplBase<char*>;
template class DMRecordImpl<const char*>;
template class DMRecordImpl<char*>;

} // namespace deltamain
} // namespace store
} // namespace tell
