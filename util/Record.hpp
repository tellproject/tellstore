#pragma once

#include <cstdint>
#include <cstddef>
#include <type_traits>
#include <crossbow/string.hpp>
#include <vector>

namespace tell {
namespace store {
namespace impl {

enum class FieldType : uint16_t {
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
        }
    }
    const crossbow::string& name() const {
        return mName;
    }
    FieldType type() const {
        return mType;
    }
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
*  - A padding to the next multiple of pointer size
*  - The row Data - it is important to understand, that one needs to
*    know the schema in order to be able to parse the record data.
*    The Table schema needs to be stored somewhere (usually in the
*    first page of the table).
*/
class Record {
private:
    char* data;
};


} // namespace tell
} // namespace store
} // namespace impl
