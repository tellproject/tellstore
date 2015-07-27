#pragma once

#include <tellstore/GenericTuple.hpp>

#include <crossbow/logger.hpp>
#include <crossbow/string.hpp>

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <type_traits>
#include <vector>
#include <unordered_map>

namespace tell {
namespace store {

enum class TableType : uint8_t {
    UNKNOWN,
    TRANSACTIONAL,
    NON_TRANSACTIONAL,
};

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

enum class PredicateType : uint8_t {
    EQUAL,
    NOT_EQUAL,
    LESS,
    LESS_EQUAL,
    GREATER,
    GREATER_EQUAL,
    LIKE,
    NOT_LIKE,
    IS_NULL,
    IS_NOT_NULL
};

template<class NumberType>
int cmp(NumberType left, NumberType right) {
    static_assert(std::is_floating_point<NumberType>::value ||
            std::is_integral<NumberType>::value, "cmp is only supported for number types");
    if (left < right) return -1;
    if (left > right) return 1;
    return 0;
}

class FieldBase {
protected:
    FieldType mType;
public:
    FieldBase(FieldType type) : mType(type) {}
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
                LOG_ASSERT(false, "One should never use a field of type NOTYPE");
                return false;
        }
    }

    off_t offsetInQuery() const {
        if (isFixedSized()) {
            auto sz = staticSize();
            if (sz <= 2) return 2;
            if (sz <= 4) return 4;
            else return 8;
        }
        return 8;
    }

    size_t staticSize() const
    {
        switch (mType) {
        case FieldType::NULLTYPE:
            return 0;
        case FieldType::SMALLINT:
            return sizeof(int16_t);
        case FieldType::INT:
            return sizeof(int32_t);
        case FieldType::BIGINT:
            return sizeof(int64_t);
        case FieldType::FLOAT:
            return sizeof(float);
        case FieldType::DOUBLE:
            return sizeof(double);
        case FieldType::TEXT:
            LOG_ASSERT(false, "Tried to get static size of TEXT Field, which does not have a static size");
            return std::numeric_limits<size_t>::max();
        case FieldType::BLOB:
            LOG_ASSERT(false, "Tried to get static size of BLOB Field, which does not have a static size");
            return std::numeric_limits<size_t>::max();
        case FieldType::NOTYPE:
            LOG_ASSERT(false, "One should never use a field of type NOTYPE");
            return std::numeric_limits<size_t>::max();
        }
    }

    size_t sizeOf(const char* data) const
    {
        if (isFixedSized()) return staticSize();
        switch (mType) {
        case FieldType::TEXT:
        case FieldType::BLOB:
            return *reinterpret_cast<const uint32_t*>(data) + sizeof(uint32_t);
        default:
            LOG_ASSERT(false, "Unknown type");
            return std::numeric_limits<size_t>::max();
        }
    }

    /**
     * We only support prefix and postfix like - for everything else
     * we expect the client to do the post filtering
     */
    bool strLike(uint32_t szLeft, const char* left, uint32_t szRight, const char* right) const
    {
        if (right[0] == '%') {
            // Postfix compare
            return szLeft < szRight - 1 ? false : memcmp(left + szLeft - szRight, right + 1, szRight - 1) == 0;
        }
        if (right[szRight - 1] == '%') {
            // Prefix compare
            return szLeft < szRight - 1 ? false : memcmp(left, right, szRight - 1) == 0;
        } else {
            // strequal
            return szLeft == szRight ? memcmp(left, right, szLeft) == 0 : false;
        }
        return false;
    }

    bool cmp(PredicateType type, const char* left, const char* right) const
    {
        int cmpRes;
        uint32_t szLeft, szRight;
        bool isText = false;
        bool isPositiveLike = true;
        switch (mType) {
        case FieldType::SMALLINT:
            cmpRes = tell::store::cmp(*reinterpret_cast<const int16_t*>(left),
                    *reinterpret_cast<const int16_t*>(right));
            goto NUMBER_COMPARE;
        case FieldType::INT:
            cmpRes = tell::store::cmp(*reinterpret_cast<const int32_t*>(left),
                    *reinterpret_cast<const int32_t*>(right));
            goto NUMBER_COMPARE;
        case FieldType::BIGINT:
            cmpRes = tell::store::cmp(*reinterpret_cast<const int64_t*>(left),
                    *reinterpret_cast<const int64_t*>(right));
            goto NUMBER_COMPARE;
        case FieldType::FLOAT:
            cmpRes = tell::store::cmp(*reinterpret_cast<const float*>(left),
                    *reinterpret_cast<const float*>(right));
            goto NUMBER_COMPARE;
        case FieldType::DOUBLE:
            cmpRes = tell::store::cmp(*reinterpret_cast<const double*>(left),
                    *reinterpret_cast<const double*>(right));
            goto NUMBER_COMPARE;
        case FieldType::TEXT:
            isText = true;
        case FieldType::BLOB:
            szLeft = *reinterpret_cast<const uint32_t*>(left);
            szRight = *reinterpret_cast<const uint32_t*>(right);
            switch (type) {
            case PredicateType::EQUAL:
                return szLeft == szRight && memcmp(left + 4, right + 4, std::min(szLeft, szRight)) == 0;
            case PredicateType::NOT_EQUAL:
                return szLeft != szRight || memcmp(left + 4, right + 4, std::min(szLeft, szRight)) != 0;
            case PredicateType::LESS:
                cmpRes = memcmp(left + 4, right + 4, std::min(szLeft, szRight));
                return cmpRes < 0 || (cmpRes == 0 && szLeft < szRight);
            case PredicateType::LESS_EQUAL:
                cmpRes = memcmp(left + 4, right + 4, std::min(szLeft, szRight));
                return cmpRes < 0 || (cmpRes == 0 && szLeft <= szRight);
            case PredicateType::GREATER:
                cmpRes = memcmp(left + 4, right + 4, std::min(szLeft, szRight));
                return cmpRes > 0 || (cmpRes == 0 && szLeft > szRight);
            case PredicateType::GREATER_EQUAL:
                cmpRes = memcmp(left + 4, right + 4, std::min(szLeft, szRight));
                return cmpRes > 0 || (cmpRes == 0 && szLeft >= szRight);
            case PredicateType::NOT_LIKE:
                isPositiveLike = false;
            case PredicateType::LIKE:
                if (!isText) {
                    LOG_ERROR("Can not do LIKE on Blob");
                    std::terminate();
                }
                return strLike(szLeft, left + 4, szRight, right + 4) == isPositiveLike;
            default:
                LOG_ERROR("Can not do this kind of comparison on numeric types");
                std::terminate();
            }
        default:
            LOG_ERROR("Unknown type");
            std::terminate();
        }
        return false;
NUMBER_COMPARE:
        switch (type) {
        case PredicateType::EQUAL:
            return cmpRes == 0;
        case PredicateType::NOT_EQUAL:
            return cmpRes != 0;
        case PredicateType::LESS:
            return cmpRes < 0;
        case PredicateType::LESS_EQUAL:
            return cmpRes <= 0;
        case PredicateType::GREATER:
            return cmpRes > 0;
        case PredicateType::GREATER_EQUAL:
            return cmpRes >= 0;
        default:
            LOG_ERROR("Can not do this kind of comparison on numeric types");
            std::terminate();
        }
    }
};

class Field : public FieldBase {
    friend class Record;
private:
    crossbow::string mName;
    bool mNotNull = false;
    char* mData = nullptr;
public:
    Field()
        : FieldBase(FieldType::NOTYPE) {
    }

    Field(FieldType type, const crossbow::string& name, bool notNull)
        : FieldBase(type), mName(name), mNotNull(notNull) {
    }

    Field(Field&& f);
    Field(const Field& f);

    Field& operator=(Field&& other);
    Field& operator=(const Field& other);
public:

    const crossbow::string& name() const {
        return mName;
    }

    FieldType type() const {
        return mType;
    }

    size_t defaultSize() const;
    size_t sizeOf(const boost::any& value) const;

    bool isNotNull() const {
        return mNotNull;
    }

    crossbow::string stringValue() const;
};


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
* - 1 byte: Type of the table
* - 1 byte: 1 if there are only columns which are declared NOT NULL,
*           0 otherwise
* - For each column:
*   - 2 bytes: type of column
*   - The name of the column, which is a string formatted like this:
*     - 2 bytes: size of the string in bytes (not in characters! this
*       string is not Unicode aware)
*/
class Schema {
private:
    TableType mType = TableType::UNKNOWN;
    bool mAllNotNull = true;
    std::vector<Field> mFixedSizeFields;
    std::vector<Field> mVarSizeFields;
public:
    Schema() = default;

    Schema(TableType type)
            : mType(type) {
    }

    Schema(const char* ptr);

    bool addField(FieldType type, const crossbow::string& name, bool notNull);

    char* serialize(char* ptr) const;

    size_t schemaSize() const;

    TableType type() const {
        return mType;
    }

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
    using id_t = uint16_t;
private:
    Schema mSchema;
    std::unordered_map<crossbow::string, id_t> mIdMap;
    std::vector<std::pair<Field, int32_t>> mFieldMetaData;
public:
    Record() = default;

    Record(Schema schema);

    const Schema& schema() const {
        return mSchema;
    }

    size_t sizeOfTuple(const GenericTuple& tuple) const;

    size_t sizeOfTuple(const char* ptr) const;

    bool idOf(const crossbow::string& name, id_t& result) const;

    const char* data(const char* const ptr, id_t id, bool& isNull, FieldType* type = nullptr) const;

    char* create(char* result, const GenericTuple& tuple, uint32_t recSize) const;
    char* create(const GenericTuple& tuple, size_t& size) const;

    /**
    * These methods are NOT thread safe.
    */
    char* data(char* const ptr, id_t id, bool& isNull, FieldType* type = nullptr);

    const std::pair<Field, int32_t>& getFieldMeta(id_t id) const {
        return mFieldMetaData.at(id);
    }
    Field getField(char* const ptr, id_t id);
    Field getField(char* const ptr, const crossbow::string& name);
};

} // namespace store
} // namespace tell
