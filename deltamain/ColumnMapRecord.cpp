/**
 * This file contains the parts of Record.cpp which are specific to the column map format.
 *
 * The memory layout of a column-map MV-DMRecord depends on the memory layout of a
 * column map page which is layed out the following way:
 *
 * - capacity: int32 to store the number of records that whould actually fit
 *   into the columns part of the storage. However, as we do not know in advance
 *   how much space the var-sized values will need, it might be that the actual
 *   count of records is smaller than the capacity.
 * - count: int32 to store the number of records that are actually stored in
 *   this page.
 * - key-version column: an array of size capcity of 16-byte values in format:
 *   |key (8 byte)|version (8 byte)|
 * - newest-pointers: an array of size capacity of 8-byte pointers to newest
 *   versions of records in the logs (as their is only one newest ptr per MV
 *   record and not per record version, only the first newestptr of every MV
 *   record is valid)
 * - null-bitmatrix: a bitmatrix of size capacity x (|Columns|+7)/8 bytes
 * - var-size-meta-data column: an array of size capacity of signed 4-byte values
 *   indicating the total size of all var-sized values of each record. This is
 *   used to allocate enough space for a record on a get request.
 *   MOREOVER: We set this size to zero to denote a version of a deleted tuple
 *   and to a negative number if the tuple is marked as reverted and will be
 *   deleted at the next GC phase. In that case, the absolute denotes the size.
 * - fixed-sized data columns: for each column there is an array of size
 *   capacity  x value-size (4 or 8 bytes, as defined in schema)
 * - var-sized data columns: for each colum there is an array of
 *   capacity  x 8 bytes in format:
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
 * together and ordered by version DESC (which facilitates get-operations).
 */

namespace impl {

/**
 * Given a reference to table, computes the beginning of the page (basePtr),
 * the total number of records in this page (capacity) and the index of the
 * current record within the page (return value).
 */
inline uint32_t getBaseKnowledge(const char* data, const Table *table, const char *&basePtr, uint32_t &capacity) {
    basePtr = table->pageManager()->getPageStart(data);
    capacity = *(reinterpret_cast<const uint32_t*>(basePtr));
    return (reinterpret_cast<uint64_t>(data-2-8-reinterpret_cast<uint64_t>(basePtr)) / 16);
}

/**
 * As we always use computeBaseKnowledge in the same way, we have a macro for that.
 */
#define COMPUTE_BASE_KNOWLEDGE(data, table) const char *basePtr; \
    uint32_t capacity; \
    uint32_t index; \
    index = getBaseKnowledge(data, table, basePtr, capacity);

/**
 * The following convenience functions are used to get pointers to items of interest
 * within the colum-oriented page.
 */

inline const uint32_t getRecordCount(const char *basePtr) {
    return *(reinterpret_cast<uint32_t*>(const_cast<char *>(basePtr + 4)));
}

inline const char *getKeyVersionPtrAt(const uint32_t index, const char * basePtr) {
    return basePtr + 8 + (index*16);
}

inline const uint64_t *getKeyAt(const uint32_t index, const char * basePtr) {
    return reinterpret_cast<const uint64_t*>(getKeyVersionPtrAt(index, basePtr));
}

inline const uint64_t *getVersionAt(const uint32_t index, const char * basePtr) {
    return reinterpret_cast<const uint64_t*>(getKeyVersionPtrAt(index, basePtr) + 8);
}

inline const char *getNewestPtrAt(const uint32_t index, const char * basePtr, const uint32_t capacity) {
    return basePtr + 8 + (capacity*16) + (index*8);

}

inline size_t getNullBitMapSize(const Table *table) {
    if (table->schema().allNotNull())
        return 0;
    else
        (table->getNumberOfFixedSizedFields() + table->getNumberOfVarSizedFields() + 7) / 8;
}

inline char *getNullBitMapAt(const uint32_t index, const char * basePtr, const uint32_t capacity, const size_t nullBitMapSize) {
    return const_cast<char *>(basePtr + 8 + (capacity*24) + (index*nullBitMapSize));
}

inline const int32_t *getVarsizedLenghtAt(const uint32_t index, const char * basePtr, const uint32_t capacity, const size_t nullBitMapSize) {
    return reinterpret_cast<const int32_t*>(basePtr + 8 + (capacity*(24 + nullBitMapSize)) + (index*4));
}

inline char *getColumnNAt(const Table *table, const uint32_t N, const uint32_t index, const char * basePtr, const uint32_t capacity, const size_t nullBitMapSize) {
    // end of var-sized = beginning of fixed-sized columns
    char *res = const_cast<char *>(reinterpret_cast<const char *>(getVarsizedLenghtAt(capacity, basePtr, capacity, nullBitMapSize)));
    const uint32_t fixedSizedFields= table->getNumberOfFixedSizedFields();
    uint32_t offset = 0;
    if (N > fixedSizedFields)   // we deal with a var-sized field, but not the first of them
        offset = table->getFieldOffset(fixedSizedFields);
    else                        // we deal with a fixed-sized or the first var-sized field
        offset = table->getFieldOffset(N);
    offset -= table->getFieldOffset(0); // subtract header part (which does not exist in column format)
    res += capacity * offset;
    if (N > fixedSizedFields)
        res += capacity * (N - fixedSizedFields) * 8;
    res += index * table->getFieldSize(N);
    return res;
}

/**
 * End of convenience functions
 */

/**
 * The following macro is used to chare the common code in getNewest and casNewest.
 */
#define GET_NEWEST auto ptr = reinterpret_cast<std::atomic<uint64_t>*>(const_cast<char*>(getNewestPtrAt(index, basePtr, capacity))); \
    auto p = ptr->load(); \
    while (ptr->load() % 2) { \
        ptr = reinterpret_cast<std::atomic<uint64_t>*>(p - 1); \
        p = ptr->load(); \
    }

/**
 * TODO: For now, this function has default parameter in order for the
 * VersionIterator to compile. Once we really want to make it useful
 * we have to force the caller to give non-null arguments!
 */
template<class T>
T MVRecordBase<T>::getNewest(const Table *table, const uint32_t index, const char * basePtr, const uint32_t capacity) const {
    GET_NEWEST
    return reinterpret_cast<char*>(p);
}


template<class T>
T MVRecordBase<T>::dataPtr() {
    LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
    std::terminate();
}

template<class T>
bool MVRecordBase<T>::isValidDataRecord() const {
    LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
    std::terminate();
}

template<class T>
void MVRecordBase<T>::revert(uint64_t version) {
    LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
    std::terminate();
}

template<class T>
bool MVRecordBase<T>::casNewest(const char* expected, const char* desired, const Table *table) const {
    COMPUTE_BASE_KNOWLEDGE(mData, table)
    GET_NEWEST
    uint64_t exp = reinterpret_cast<const uint64_t>(expected);
    uint64_t des = reinterpret_cast<const uint64_t>(desired);
    if (p != exp) return false;
    return ptr->compare_exchange_strong(exp, des);
}

template<class T>
int32_t MVRecordBase<T>::getNumberOfVersions() const {
    LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
    std::terminate();
}

template<class T>
const uint64_t* MVRecordBase<T>::versions() const {
    LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
    std::terminate();
}

template<class T>
const int32_t* MVRecordBase<T>::offsets() const {
    LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
    std::terminate();
}

template<class T>
uint64_t MVRecordBase<T>::size() const {
    LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
    std::terminate();
}

template<class T>
bool MVRecordBase<T>::needsCleaning(uint64_t lowestActiveVersion, InsertMap& insertMap) const {
    LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
    std::terminate();
}

/**
 * BE CAREFUL: in contrast to the row-oriented version, this call actually
 * allocates new delta (in sequential row-format) and does not only return
 * a pointer.
 */
template<class T>
const char *MVRecordBase<T>::data(const commitmanager::SnapshotDescriptor& snapshot,
                 size_t& size,
                 uint64_t& version,
                 bool& isNewest,
                 bool& isValid,
                 bool* wasDeleted,
                 const Table *table
) const {
    COMPUTE_BASE_KNOWLEDGE(mData, table)
    const size_t nullBitMapSize = getNullBitMapSize(table);

    auto newest = getNewest(table, index, basePtr, capacity);
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
    int32_t *varLength = const_cast<int32_t *>(getVarsizedLenghtAt(index, basePtr, capacity, nullBitMapSize));
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
                + *getVarsizedLenghtAt(index, basePtr, capacity, getNullBitMapSize(table));
        std::unique_ptr<char[]> buf(new char[recordSize]);  //TODO: is this what it should look like? Am I using the right allocator?
        char *res = buf.get();
        // copy fixed-sized columns
        char *src;
        char *dest = res;
        for (uint i = 0; i < fixedSizeFields; i++)
        {
            src = const_cast<char*>(getColumnNAt(table, i, index, basePtr, capacity, nullBitMapSize));
            auto fieldSize = table->getFieldSize(i);
            memcpy(dest, src, fieldSize);
            dest += fieldSize;
        }
        // copy var-sized colums in a batch
        src = const_cast<char *>(getColumnNAt(table, fixedSizeFields, index, basePtr, capacity, nullBitMapSize));
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

template<class T>
typename MVRecordBase<T>::Type MVRecordBase<T>::typeOfNewestVersion(bool& isValid) const {
    LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
    std::terminate();
}

template<class T>
void MVRecordBase<T>::collect(impl::VersionMap&, bool&, bool&) const {
    LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
    std::terminate();
}

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

//Implementations of MVRecord<char*> ############################################

uint64_t *MVRecord<char*>::versions() {
    LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
    std::terminate();
}

int32_t *MVRecord<char*>::offsets() {
    LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
    std::terminate();
}

char *MVRecord<char*>::dataPtr() {
    LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
    std::terminate();
}

bool MVRecord<char*>::update(char* next,
            bool& isValid,
            const commitmanager::SnapshotDescriptor& snapshot) {
    LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
    std::terminate();
}


} // namespace impl
