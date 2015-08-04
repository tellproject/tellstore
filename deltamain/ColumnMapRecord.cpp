/**
 * This file contains the parts of Record.cpp which are specific to the column map format.
 *
 * The memory layout of a column-map MV-DMRecord depends on the memory layout of a
 * column map page which is layed out the following way:
 *
 * - count: int32 to store the number of records that are stored in this page.
 *   We also define count^ which is: count^ = 2*((count+1)/2)
 * - GC-flag (4 byte): a non-zero value indicates that the page is currently
 *   being constructed by gc and hence variable-sized values have to be
 *   retrieved from a different page.
 * - key-version column: an array of size count of 16-byte values in format:
 *   |key (8 byte)|version (8 byte)|
 * - newest-pointers: an array of size count of 8-byte pointers to newest
 *   versions of records in the logs (as their is only one newest ptr per MV record
 *   and not per record version, only the first newestptr of every MV record is valid)
 * - null-bitmatrix: a bitmatrix of size count x (|Columns|+7)/8 bytes
 * - var-size-meta-data column: an array of size count of signed 4-byte values
 *   indicating the total size of all var-sized values of each record. This is
 *   used to allocate enough space for a record on a get request.
 *   MOREOVER: We set this size to zero to denote a version of a deleted tuple
 *   and to a negative number if the tuple is marked as reverted and will be
 *   deleted at the next GC phase. In that case, the absolute denotes the size.
 * - fixed-sized data columns: for each column there is an array of size
 *   count^ x value-size (4 or 8 bytes, as defined in schema)
 * - var-sized data columns: for each colum there is an array of
 *   count x 8 bytes in format:
 *   |4-byte-offset from page start into var-sized heap|4-byte prefix of value|
 * - var-sized heap: values referred from var-sized columns in the format
 *   |4-byte size (including the size field)|value|
 *
 * Pointers into a page (e.g. from log) point to the first key/version entry in
 * the key-version column, but have the second lowest bit set to 1 (in order to
 * make clear it is a columnMap-MV record). This bit has to be unset (by subtracting
 * 2) in order to get the correct address.
 *
 * MV records are stored as single records in such a way that they are clustered
 * together and ordered by version DESC (which makes get operations simpler).
 */

//the following includes are just for convenience when coding, not actually needed
#include <stdint.h>
#include "deltamain/Table.hpp"

namespace impl {

template<class T>
class MVRecordBase {
protected:
    T mData;

public:
    using Type = typename DMRecordImplBase<T>::Type;
    MVRecordBase(T data) : mData(data) {}

    /**
     * Given a reference to table, computes the beginning of the page (basePtr),
     * the total number of records in this page (recordCount) and the index of the
     * current record within the page (return value).
     */
    inline uint32_t getBaseKnowledge(const Table *table, const char *&basePtr, uint32_t &recordCount) const {
        basePtr = table->pageManager()->getPageStart(mData);
        recordCount = *(reinterpret_cast<const uint32_t*>(basePtr));
        return (reinterpret_cast<uint64_t>(mData-2-8-reinterpret_cast<uint64_t>(basePtr)) / 16);
    }

    /**
     * The following convenience functions are used to get pointers to items of interest
     * within the colum-oriented page.
     */

    inline const char *getKeyVersionPtrAt(const uint32_t index, const char * basePtr) const {
        return basePtr + 8 + (index*16);
    }

    inline const uint64_t *getKeyAt(const uint32_t index, const char * basePtr) const {
        return reinterpret_cast<const uint64_t*>(getKeyVersionPtrAt(index, basePtr));
    }

    inline const uint64_t *getVersionAt(const uint32_t index, const char * basePtr) const {
        return reinterpret_cast<const uint64_t*>(getKeyVersionPtrAt(index, basePtr) + 8);
    }

    inline const char *getNewestPtrAt(const uint32_t index, const char * basePtr, const uint32_t recordCount) const {
        return basePtr + 8 + (recordCount*16) + (index*8);

    }

    inline size_t getNullBitMapSize(const Table *table) const {
        if (table->schema().allNotNull())
            return 0;
        else
            (table->getNumberOfFixedSizedFields() + table->getNumberOfVarSizedFields() + 7) / 8;
    }

    inline char *getNullBitMapAt(const uint32_t index, const char * basePtr, const uint32_t recordCount, const size_t nullBitMapSize) const {
        return basePtr + 8 + (recordCount*24) + (index*nullBitMapSize);
    }

    inline const int32_t *getVarsizedLenghtAt(const uint32_t index, const char * basePtr, const uint32_t recordCount, const size_t nullBitMapSize) const {
        return reinterpret_cast<const int32_t*>(basePtr + 8 + (recordCount*(24 + nullBitMapSize)) + (index*4));
    }

    inline uint32_t getRecordCountHat(const uint32_t recordCount) const {
        return ((recordCount + 1) / 2) * 2;
    }

    inline char *getColumnNAt(const Table *table, const uint32_t N, const uint32_t index, const char * basePtr, const uint32_t recordCount, const size_t nullBitMapSize) const {
        // end of var-sized = beginning of fixed-sized columns
        char *res = const_cast<char *>(reinterpret_cast<const char *>(getVarsizedLenghtAt(recordCount, basePtr, recordCount, nullBitMapSize)));
        const uint32_t fixedSizedFields= table->getNumberOfFixedSizedFields();
        uint32_t offset = 0;
        if (N > fixedSizedFields)   // we deal with a var-sized field, but not the first of them
            offset = table->getFieldOffset(fixedSizedFields);
        else                        // we deal with a fixed-sized or the first var-sized field
            offset = table->getFieldOffset(N);
        offset -= table->getFieldOffset(0); // subtract header part (which does not exist in column format)
        res += getRecordCountHat(recordCount) * offset;
        if (N > fixedSizedFields)
            res += recordCount * (N - fixedSizedFields);
        res += index * table->getFieldSize(N);
        return res;
    }

#define COMPUTE_BASE_KNOWLEDGE(table) const char *basePtr; \
    uint32_t recordCount; \
    uint32_t index; \
    index = getBaseKnowledge(table, basePtr, recordCount);

    /**
     * End of convenience functions
     */

    /**
     * TODO: For now, this function has default parameter in order for the
     * VersionIterator to compile. Once we really want to make it useful
     * we have to force the caller to give non-null arguments!
     */
    T getNewest(const Table *table = nullptr, const uint32_t index = 0, const char * basePtr = nullptr, const uint32_t recordCount = 0) const {
        // The pointer format is like the following:
        // If (ptr % 2) -> this is a link, we need to
        //      follow this version.
        // else This is just a normal version - but it might be
        //      a MVRecord
        //
        // This const_cast is a hack - we might fix it
        // later
        auto ptr = reinterpret_cast<std::atomic<uint64_t>*>(const_cast<char*>(getNewestPtrAt(index, basePtr, recordCount)));
        auto p = ptr->load();
        while (ptr->load() % 2) {
            // we need to follow this pointer
            ptr = reinterpret_cast<std::atomic<uint64_t>*>(p - 1);
            p = ptr->load();
        }
        return reinterpret_cast<char*>(p);
    }


    T dataPtr() {
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
        std::terminate();
    }

    bool isValidDataRecord() const {
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
        std::terminate();
    }

    void revert(uint64_t version) {
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
        std::terminate();
    }

//    bool casNewest(const char* expected, const char* desired) const {
//        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
//        std::terminate();
//    }

    int32_t getNumberOfVersions() const {
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
        std::terminate();
    }

    const uint64_t* versions() const {
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
        std::terminate();
    }

    const int32_t* offsets() const {
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
        std::terminate();
    }

    uint64_t size() const {
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
        std::terminate();
    }

    bool needsCleaning(uint64_t lowestActiveVersion, InsertMap& insertMap) const {
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
        std::terminate();
    }

    /**
     * BE CAREFUL: in contrast to the row-oriented version, this call actually
     * allocates new delta (in sequential row-format) and does not only return
     * a pointer.
     */
    const char* data(const commitmanager::SnapshotDescriptor& snapshot,
                     size_t& size,
                     uint64_t& version,
                     bool& isNewest,
                     bool& isValid,
                     bool* wasDeleted,
                     const Table *table
    ) const {
        COMPUTE_BASE_KNOWLEDGE(table)
        const size_t nullBitMapSize = getNullBitMapSize(table);

        auto newest = getNewest(table, index, basePtr, recordCount);
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

        bool found = false;
        uint64_t *key = const_cast<uint64_t *>(getKeyAt(index, basePtr));
        int32_t *varLength = const_cast<int32_t *>(getVarsizedLenghtAt(index, basePtr, recordCount, nullBitMapSize));
        for (; ; index++, key += 2, varLength++) {
            if (*varLength < 0) continue;
            isValid = true;
            if (snapshot.inReadSet(key[1])) {   // key[0]: key, key[1]: version
                version = key[1];
                found = true;
                break;
            }
            isNewest = false;
            if (key[2] != key[0])
                break;  // loop exit condition
        }
        // index, varLength and key should have the right values

        if (!found) {
            if (wasDeleted) *wasDeleted = false;
            return nullptr;
        }

        if (*varLength != 0)
        {
            if (wasDeleted) *wasDeleted = false;

            auto fixedSizeFields = table->getNumberOfFixedSizedFields();
            uint32_t recordSize = table->getFieldOffset(table->getNumberOfFixedSizedFields())
                    + *getVarsizedLenghtAt(index, basePtr, recordCount, getNullBitMapSize(table));
            std::unique_ptr<char[]> buf(new char[recordSize]);  //TODO: is this what it should look like? Am I using the right allocator?
            char *res = buf.get();
            // copy fixed-sized columns
            char *src;
            char *dest = res;
            for (uint i = 0; i < fixedSizeFields; i++)
            {
                src = const_cast<char*>(getColumnNAt(table, i, index, basePtr, recordCount, nullBitMapSize));
                auto fieldSize = table->getFieldSize(i);
                memcpy(dest, src, fieldSize);
                dest += fieldSize;
            }
            // copy var-sized colums in a batch
            src = const_cast<char *>(getColumnNAt(table, fixedSizeFields, index, basePtr, recordCount, nullBitMapSize));
            src = const_cast<char *>(basePtr + *(reinterpret_cast<uint32_t *>(src)));   // pointer to first field in var-sized heap
            memcpy(dest, src, *varLength);

            // release buffer (which should hopefully not be garbage-collected too early)
            buf.release();
            return res;
        }
        if (wasDeleted)
            *wasDeleted = true;
        return nullptr;
    }

    Type typeOfNewestVersion(bool& isValid) const {
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
        std::terminate();
    }

    void collect(impl::VersionMap&, bool&, bool&) const {
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
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
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
        std::terminate();
    }
    void writePrevious(const char*) {
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
        std::terminate();
    }
    void writeData(size_t, const char*) {
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
        std::terminate();
    }

    uint64_t* versions() {
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
        std::terminate();
    }

    int32_t* offsets() {
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
        std::terminate();
    }

    char* dataPtr() {
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
        std::terminate();
    }

    bool update(char* next,
                bool& isValid,
                const commitmanager::SnapshotDescriptor& snapshot) {
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
        std::terminate();
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
    LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
    std::terminate();
}

} // namespace impl
