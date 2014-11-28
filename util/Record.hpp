#pragma once

#include <cstdint>
#include <cstddef>
#include <type_traits>
#include <crossbow/string.hpp>
#include <vector>
#include <unordered_map>
#include "Logging.hpp"

namespace tell {
namespace store {
namespace impl {

enum class FieldType : uint16_t {
    NOTYPE = 0,
    SMALLINT = 1,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    TEXT, // this is used for CHAR and VARCHAR as well
    BLOB
};

class Field {
private:
    FieldType mType;
    crossbow::string mName;
public:
    Field() : mType(FieldType::NOTYPE) {}
    Field(FieldType type, const crossbow::string& name)
            : mType(type),
              mName(name)
    {}
    bool isFixedSized() const {
        switch (mType) {
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
};

template<typename Enum>
auto to_underlying(Enum e) -> typename std::underlying_type<Enum>::type {
    return static_cast<typename std::underlying_type<Enum>::type>(e);
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
    Schema() {}
    Schema(const char* ptr);
    bool addField(FieldType type, const crossbow::string& name, bool notNull);
    char* serialize(char* ptr) const;
    size_t schemaSize() const;
    bool allNotNull() const { return mAllNotNull; }
    const std::vector<Field>& fixedSizeFields() const { return mFixedSizeFields; }
    const std::vector<Field>& varSizeFields() const { return mVarSizeFields; }
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

    /**
    * Sets the field at id to @isNull. Returns true iff the field
    * was NULL before.
    */
    bool setNull(id_t id, bool isNull);
};

/**
* This can be used for storages, where the versions used for snapshot isolation
* are kept together. The format of a record is like follows:
*
* - 4 bytes: size of the tuples (including this field, headers, all versions etc)
* - 4 bytes: number of versions
* - An array of 8 byte integers of all version numbers
* - An array of 4 byte integers, where integer at offset i is the offset to
*   record with version[i].
* - A 4 byte padding if |version| % 8 != 0
*
* The versions are ordered decremental - the means the newest version comes first
*/
struct MultiVersionRecord {
};


} // namespace tell
} // namespace store
} // namespace impl
