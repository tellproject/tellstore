#pragma once

#include <cstdint>
#include <cstddef>
#include <type_traits>
#include <crossbow/string.hpp>
#include <vector>
#include <unordered_map>
#include "Logging.hpp"
#include "SnapshotDescriptor.hpp"

namespace tell {
namespace store {

enum class FieldType
    : uint16_t {
    NOTYPE = 0,
    NULLTYPE = 1,
    SMALLINT = 2,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    TEXT, // this is used for CHAR and VARCHAR as well
    BLOB
};

class Record;

class Field {
    friend class Record;
private:
    FieldType mType;
    crossbow::string mName;
    bool mNotNull = false;
    char* mData = nullptr;
public:
    Field()
        : mType(FieldType::NOTYPE) {
    }

    Field(FieldType type, const crossbow::string& name, bool notNull)
        : mType(type), mName(name), mNotNull(notNull) {
    }

    Field(Field&& f);
    Field(const Field& f);

    Field& operator=(Field&& other);
    Field& operator=(const Field& other);
public:

    bool isFixedSized() const {
        switch (mType) {
            case FieldType::NULLTYPE:
                return true;
            case FieldType::SMALLINT:
            case FieldType::INT:
            case FieldType::BIGINT:
            case FieldType::FLOAT:
            case FieldType::DOUBLE:
                return true;
            case FieldType::TEXT:
            case FieldType::BLOB:
                return false;
            case FieldType::NOTYPE:
                assert(false);
                LOG_ERROR("One should never use a field of type NOTYPE");
                return false;
        }
    }

    const crossbow::string& name() const {
        return mName;
    }

    FieldType type() const {
        return mType;
    }

    size_t staticSize() const;

    bool isNotNull() const {
        return mNotNull;
    }

    crossbow::string stringValue() const;
};

template<typename Enum>
constexpr auto to_underlying(Enum e) -> typename std::underlying_type<Enum>::type {
    return static_cast<typename std::underlying_type<Enum>::type>(e);
}

template<typename Enum>
constexpr Enum from_underlying(typename std::underlying_type<Enum>::type s) {
    return static_cast<Enum>(s);
}

/**
* This class is used to create and parse a table schema.
*
* The schema orders the fields in a way, that all fixed size columns
* come first. This allows for faster column lookup if a fixed size
* column is needed.
*
* The format is like follows:
* - 4 bytes: size of schema data structure (including size)
* - 2 bytes: number of columns
* - 1 byte: 1 if there are only columns which are declared NOT NULL,
*           0 otherwise
* - 1 bytes: padding
* - For each column:
*   - 2 bytes: type of column
*   - The name of the column, which is a string formatted like this:
*     - 2 bytes: size of the string in bytes (not in characters! this
*       string is not Unicode aware)
*/
class Schema {
private:
    bool mAllNotNull = true;
    std::vector<Field> mFixedSizeFields;
    std::vector<Field> mVarSizeFields;
public:
    Schema() {
    }

    Schema(const char* ptr);

    bool addField(FieldType type, const crossbow::string& name, bool notNull);

    char* serialize(char* ptr) const;

    size_t schemaSize() const;

    bool allNotNull() const {
        return mAllNotNull;
    }

    const std::vector<Field>& fixedSizeFields() const {
        return mFixedSizeFields;
    }

    const std::vector<Field>& varSizeFields() const {
        return mVarSizeFields;
    }
};

/**
* This class implements the physical representation of a tuple.
* Note that this class does not handle multiple versions for
* snapshot isolation, since this has to be handled in the
* table itself (it depends on the approach).
*
* The format look as follows (in the following order):
*  - 4 bytes: size of the tuple (including the header, padding, etc)
*  - NULL bitmap (size of bitmap is (|Columns|+7)/8 bytes)
*    This is omitted, if all columns are declared as NOT NULL
*  - A padding to the next multiple of pointer size
*  - The row Data - it is important to understand, that one needs to
*    know the schema in order to be able to parse the record data.
*    The Table schema needs to be stored somewhere (usually in the
*    first page of the table).
*/
class Record {
public:
    using id_t = size_t;
private:
    const Schema& mSchema;
    std::unordered_map<crossbow::string, id_t> mIdMap;
    std::vector<std::pair<Field, off_t>> mFieldMetaData;
public:
    Record(const Schema& schema);

    bool idOf(const crossbow::string& name, id_t& result) const;

    const char* data(const char* const ptr, id_t id, bool& isNull, FieldType* type = nullptr) const;

    /**
    * These methods are NOT thread safe.
    */
    char* data(char* const ptr, id_t id, bool& isNull, FieldType* type = nullptr);

    Field getField(char* const ptr, id_t id);
    Field getField(char* const ptr, const crossbow::string& name);

    uint32_t getSize(const char* data) const {
        return *reinterpret_cast<const uint32_t*>(data);
    }
};

/**
* This can be used for storages, where the versions used for snapshot isolation
* are kept together. The format of a record is like follows:
*
* - 4 bytes: size of the tuples (including this field, headers, all versions etc)
* - 4 bytes: number of versions
* - An array of 8 byte integers of all version numbers
* - An array of 4 byte integers, where integer at offset i is the offset to
*   record with version[i]. If the value is 0, it means that the record got deleted
*   in this version.
* - A 4 byte padding if |version| % 8 != 0
*
* The versions are ordered decremental - the means the newest version comes first
*/
struct MultiVersionRecord {
    static const char* getRecord(const SnapshotDescriptor& desc, const char* record, bool& isNewest);
    static uint64_t getSmallestVersion(const char* record);
    static uint64_t getBiggestVersion(const char* record);
    static uint32_t getNumberOfVersions(const char* data) {
        return *reinterpret_cast<const uint32_t*>(data + 4);
    }

    static uint32_t getSize(const char* data) {
        return *reinterpret_cast<const uint32_t*>(data);
    }

    /**
    * Removed all versions < minVersion from the set. This will return the old
    * size of the record.
    * If the 0 is returned, it means that the function did not change the record,
    * but it can be completely removed. This is the case, iff:
    *  (i)  All versions are smaller than minVersion
    *  (ii) The newest version was a deletion
    */
    static uint32_t compact(char* record, uint64_t minVersion);
};

} // namespace store
} // namespace tell
