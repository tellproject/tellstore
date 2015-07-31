/**
 * This file contains the parts of Record.cpp which are specific to the column map format.
 *
 * The memory layout of a column-map MV-DMRecord depends on the memory layout of a
 * column map page which is layed out the following way:
 *
 * - count: int32 to store the number of records that are stored in this page
 *   a negative count indicates that the page is currently being constructed by
 *   gc and hence variable-sized values have to be retrieved from a different
 *   page.
 * - count^: just stored as convenience (as we would padd anyway):
 *   count^ = 2*((count+1)/2)
 * - newest-pointers: an array of size count of 8-byte pointers to newest
 *   versions of records in the logs
 * - null-bitmatrix: a bitmatrix of size count x (|Columns|+7)/8 bytes
 * - padding to the next multiple of 8
 * - key-version column: an array of size count of 16-byte values in format:
 *   |key (8 byte)|version (8 byte)|
 * - fixed-sized data columns: for each column there is an array of size
 *   count^ x value-size (4 or 8 bytes, as defined in schema)
 * - var-sized data columns: for each colum there is an array of
 *   count^ x 8 bytes in format:
 *   |4-byte-offset from page start into var-sized heap|4-byte prefix of value|
 * - var-sized heap: values referred from var-sized columns in the format
 *   |4-byte size (including the size field)|value|
 *
 * Pointers into a page (e.g. from log) point to the first key/version entry in
 * the key-version column, but have the second lowest bit set to 1 (in order to
 * make clear it is a columnMap-MV record). This bit has to be unset in order to
 * get the correct address.
 */

namespace impl {

template<class T>
class MVRecordBase {
protected:
    T mData;
#if defined USE_COLUMN_MAP
    Table *mTable = nullptr;
#endif

public:
    using Type = typename DMRecordImplBase<T>::Type;
    MVRecordBase(
            T data
#if defined USE_COLUMN_MAP
            ,
            Table *table = nullptr
#endif
            ) :
        mData(data)
#if defined USE_COLUMN_MAP
        ,
        mTable(table)
#endif
    {}

    T getNewest() const {
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
        std::terminate();
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

    bool casNewest(const char* expected, const char* desired) const {
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
        std::terminate();
    }

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

    const char* data(const commitmanager::SnapshotDescriptor& snapshot,
                     size_t& size,
                     uint64_t& version,
                     bool& isNewest,
                     bool& isValid,
                     bool* wasDeleted) const {
        LOG_ERROR("You are not supposed to call this on a columMap MVRecord");
        std::terminate();
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
