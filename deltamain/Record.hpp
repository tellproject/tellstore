#pragma once
#include <cstddef>
#include <cstdint>
#include <map>
#include <atomic>

#include <config.h>
#include <crossbow/enum_underlying.hpp>
#include <util/IteratorEntry.hpp>

#include "InsertMap.hpp"

#include "rowstore/RowStoreVersionIterator.in.cpp"

namespace tell {
namespace commitmanager {
class SnapshotDescriptor;
} // namespace commitmanager

namespace store {

namespace deltamain {

enum class RecordType : uint8_t {
    LOG_INSERT,
    LOG_UPDATE,
    LOG_DELETE,
    MULTI_VERSION_RECORD
};

class Table;

namespace impl {
struct VersionHolder {
    const char* record;
    RecordType type;
    size_t size;
    std::atomic<const char*>* nextPtr;
};
using VersionMap = std::map<uint64_t, VersionHolder>;

}

/**
 * // TODO: Implement revert
 *
 * This class handles Records which are either in the
 * log or in a table. The base pointer must be set in
 * a way, that it is able to find all relevant versions
 * of the record from there. The general memory layout
 * of a DMRecord looks like follows:
 *
 * - 1 byte: Record::Type
 * - meta data specific for the record type
 * - The data (if not delete)
 *
 * For the memory layout of log records:
 * PLEASE consult the specific comments in LogRecord.hpp
 *
 * For the memory layout of a MV-DMRecord:
 * PLEASE consult the specific comments in RowStoreRecord.hpp,
 * resp. ColumnMapRecord.hpp.
 *
 * This class comes in to flavors: const and non-const.
 * The non-const version provides also functionality for
 * writing to the memory.
 */

/**
 * if we use columnMap, MV records need a special treatment for data pointers
 * as pointers point directly to a key but have the second last bit set to 1 to
 * indicate that this is an MV and not any kind of log record.
 */
#if defined USE_COLUMN_MAP
        #define IS_MV_RECORD ((reinterpret_cast<uint64_t>(mData) >> 1) % 2)
#endif

template<class T>
class DMRecordImplBase {
    using target_type = typename std::remove_pointer<T>::type;
    static_assert(sizeof(target_type) == 1, "only pointers to one byte size types are supported");
protected:
    T mData;
public:
    using Type = RecordType;

    DMRecordImplBase(T data) : mData(data) {}

    RecordType type() const {
#if defined USE_COLUMN_MAP
// if we use columnMap, finding the type is slightly more tricky
        if (IS_MV_RECORD)
            return RecordType::MULTI_VERSION_RECORD;
#endif
        return crossbow::from_underlying<Type>(*mData);
    }

    uint64_t key() const {
#if defined USE_COLUMN_MAP
// if we use columnMap, the key is at a different offset
        if (IS_MV_RECORD)
            return *reinterpret_cast<const uint64_t*>(mData -2);
#endif
        return *reinterpret_cast<const uint64_t*>(mData + 8);
    }

    /**
     * This will return the newest version readable in snapshot
     * or nullptr if no such version exists.
     *
     * If wasDeleted is provided, it will be set to true if there
     * is a tuple in the read set but it got deleted.
     *
     * isValid will be set to false iff all versions accessible from
     * this tuple got reverted.
     */
    const char* data(
            const commitmanager::SnapshotDescriptor& snapshot,
            size_t& size,
            uint64_t& version,
            bool& isNewest,
            bool& isValid,
            bool* wasDeleted = nullptr,
            const Table *table = nullptr,
            bool copyData = true
            ) const;


    T dataPtr();

    static size_t spaceOverhead(Type t);

    RecordType typeOfNewestVersion(bool& isValid) const;
    uint64_t size() const;
    bool needsCleaning(uint64_t lowestActiveVersion, InsertMap& insertMap) const;

    // traverses previous pointers of log record repeatedly and adds record versions
    // to the version map in ascending version order
    void collect(
            impl::VersionMap& versions,
            bool& newestIsDelete,
            bool& allVersionsInvalid) const;

    /**
     * This method will GC the record to a new location. It will then return
     * how much data it has written or 0 iff the new location is too small
     */
    uint64_t copyAndCompact(
            uint64_t lowestActiveVersion,
            InsertMap& insertMap,
            char* newLocation,
            uint64_t maxSize,
            bool& success) const;

    void revert(uint64_t version);
    /**
     * This function return true iff the underlying item is a log entry and it
     * is not a tombstone or a reverted operation.
     */
    bool isValidDataRecord() const;

    const RowStoreVersionIterator getVersionIterator(const Record *record) const;
};

template<class T>
class DMRecordImpl : public DMRecordImplBase<T> {
public:
    DMRecordImpl(T data) : DMRecordImplBase<T>(data) {}
};

/**
 * This is a specialization of the DMRecord class that
 * contains all writing operations. Most of the functions
 * in this class will only work correctly for some
 * types of records. If it is called for the wrong type,
 * it will crash at runtime.
 */
template<>
class DMRecordImpl<char*> : public DMRecordImplBase<char*> {
public:
    DMRecordImpl(char* data) : DMRecordImplBase<char*>(data) {}
public: // writing functinality
    void setType(Type type) {
        this->mData[0] = crossbow::to_underlying(type);
    }
    /**
     * This can be called on all types
     */
    void writeKey(uint64_t key);
    /**
     * This can only be called on log entries
     */
    void writeVersion(uint64_t version);
    /**
     * This can only be called on log entries
     */
    void writePrevious(const char* prev);
    /**
     * This can only be called on log entries
     */
    void writeData(size_t size, const char* data);

    bool update(char* next,
                bool& isValid,
                const commitmanager::SnapshotDescriptor& snapshot,
                const Table *table = nullptr
    );

};

extern template class DMRecordImplBase<const char*>;
extern template class DMRecordImplBase<char*>;
extern template class DMRecordImpl<const char*>;
extern template class DMRecordImpl<char*>;

using CDMRecord = DMRecordImpl<const char*>;
using DMRecord = DMRecordImpl<char*>;

} // namespace deltamain
} // namespace store
} // namespace tell
