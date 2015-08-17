#include "Record.hpp"
#include "LogRecord.hpp"
#include "rowstore/RowStoreRecord.hpp"
#include "rowstore/RowStoreVersionIterator.hpp"
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

template<class T>
const VersionIterator DMRecordImplBase<T>::getVersionIterator(const Record *record) const {
    return VersionIterator(record, this->mData);
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
