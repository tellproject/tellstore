#pragma once
#include <cstddef>
#include <cstdint>
#include <map>
#include <atomic>

#include <util/helper.hpp>

#include "InsertMap.hpp"

namespace tell {
namespace store {
struct SnapshotDescriptor;
namespace deltamain {
namespace impl {
struct VersionHolder {
    const char* record;
    size_t size;
    std::atomic<const char*>* nextPtr;
};
using VersionMap = std::map<uint64_t, VersionHolder>;
}

enum class RecordType : uint8_t {
    LOG_INSERT,
    LOG_UPDATE,
    LOG_DELETE,
    MULTI_VERSION_RECORD
};

/**
 * This class handles Records which are either in the
 * log or in a table. The base pointer must be set in
 * a way, that it is able to find all relevant versions
 * of the record from there. The memory layout of a
 * DMRecord looks like follows:
 *
 * -1 byte: Record::Type
 * -7 bytes padding if it is a log entry, otherwise it
 *  stores 3 bytes padding plus a 4 byte integer to store
 *  the number of versions
 * -8 bytes: key
 *
 *    For log entries:
 *    - 8 bytes: version
 *    - 8 bytes: pointer to a previous version. this will
 *      always be set to null, if the previous version was
 *      not an update log entry. If the previous version
 *      was an insert log entry, the only way to reach the
 *      update is via the insert entry, if it was a multi
 *      version record, we can only reach it via the 
 *      record entry itself. This is an important design
 *      decision: this way me make clear that we do not
 *      introduce cycles.
 *  
 *       For insert log entries:
 *       - 8 bytes for a next pointer
 *
 *    For multiversion records:
 *    - A pointer to the newest version
 *    - An array of version numbers
 *    - An array of size number_of_versions + 1 of 4 byte integers
 *      to store the offsets to
 *      the data for each version - this offset will be absolute.
 *      If the offset is euqal to the next offset, it means that the
 *      tuple was deleted at this version. The last offsets points to
 *      the byte after the record.
 *    - A 4 byte padding if there are an even number of versions
 *
 *  - The data (if not delete)
 *
 *  This class comes in to flavors: const and non-const.
 *  The non-const version provides also functionality for
 *  writing to the memory.
 */
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
        return from_underlying<Type>(*mData);
    }

    uint64_t key() const {
        return *reinterpret_cast<const uint64_t*>(mData + 8);
    }

    /**
     * This will return the newest version readable in snapshot
     * or nullptr if no such version exists.
     *
     * If wasDeleted is provided, it will be set to true if there
     * is a tuple in the read set but it got deleted.
     */
    const char* data(
            const SnapshotDescriptor& snapshot,
            size_t& size,
            bool& isNewest,
            bool* wasDeleted = nullptr) const;


    T dataPtr();

    static size_t spaceOverhead(Type t);

    RecordType typeOfNewestVersion() const;
    uint64_t size() const;
    bool needsCleaning(uint64_t lowestActiveVersion, InsertMap& insertMap) const;
    void collect(
            impl::VersionMap& versions,
            bool& newestIsDelete) const;

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
        this->mData[0] = to_underlying(type);
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
    /**
     * This can only be called on insert entries
     */
    void writeNextPtr(const char* next);

    bool update(char* next,
                const SnapshotDescriptor& snapshot);

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

