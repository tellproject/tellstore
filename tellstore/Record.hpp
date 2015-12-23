/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
// !b = ../common/Record.cpp
#pragma once

#include <tellstore/GenericTuple.hpp>
#include <tellstore/StdTypes.hpp>

#include <crossbow/logger.hpp>
#include <crossbow/string.hpp>
#include <crossbow/byte_buffer.hpp>

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <type_traits>
#include <vector>
#include <unordered_map>

namespace tell {
namespace store {

class Record;

template <class NumberType>
void initAgg(AggregationType type, NumberType* result) {
    static_assert(std::is_floating_point<NumberType>::value ||
            std::is_integral<NumberType>::value, "agg is only supported for number types");

    switch (type) {
    case AggregationType::MIN: {
        *result = std::numeric_limits<NumberType>::max();
    } break;

    case AggregationType::MAX: {
        *result = std::numeric_limits<NumberType>::min();
    } break;

    case AggregationType::SUM: {
        *result = 0;
    } break;

    case AggregationType::CNT: {
        *result = 0;
    } break;

    default: {
        LOG_ASSERT(false, "Unknown aggregation type");
    } break;
    }
}

class FieldBase {
protected:
    FieldType mType;
public:
    FieldBase(FieldType type) : mType(type) {}
public:

    FieldType type() const {
        return mType;
    }

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
        default:
            LOG_ASSERT(false, "Unknown type");
            return false;
        }
    }

    size_t sizeOfPredicate(const char* data) const {
        switch (mType) {
        case FieldType::NULLTYPE:
        case FieldType::SMALLINT:
        case FieldType::INT:
        case FieldType::FLOAT:
            return 8;
        case FieldType::BIGINT:
        case FieldType::DOUBLE:
            return 16;
        case FieldType::TEXT:
        case FieldType::BLOB:
            return crossbow::align(8 + *reinterpret_cast<const uint32_t*>(data + 4), 8);
        case FieldType::NOTYPE:
            LOG_ASSERT(false, "One should never use a field of type NOTYPE");
            return std::numeric_limits<size_t>::max();
        default:
            LOG_ASSERT(false, "Unknown type");
            return std::numeric_limits<size_t>::max();
        }
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
        case FieldType::BLOB:
            return sizeof(uint32_t);
        case FieldType::NOTYPE:
            LOG_ASSERT(false, "One should never use a field of type NOTYPE");
            return std::numeric_limits<size_t>::max();
        default:
            LOG_ASSERT(false, "Unknown type");
            return std::numeric_limits<size_t>::max();
        }
    }

    size_t alignOf() const {
        switch (mType) {
        case FieldType::NULLTYPE:
            return 0;
        case FieldType::SMALLINT:
            return alignof(int16_t);
        case FieldType::INT:
            return alignof(int32_t);
        case FieldType::BIGINT:
            return alignof(int64_t);
        case FieldType::FLOAT:
            return alignof(float);
        case FieldType::DOUBLE:
            return alignof(double);
        case FieldType::TEXT:
        case FieldType::BLOB:
            return alignof(uint32_t);
        case FieldType::NOTYPE:
            LOG_ASSERT(false, "One should never use a field of type NOTYPE");
            return std::numeric_limits<size_t>::max();
        default:
            LOG_ASSERT(false, "Unknown type");
            return std::numeric_limits<size_t>::max();
        }
    }

    void initAgg(AggregationType type, char* data) const {
        switch (mType) {
        case FieldType::SMALLINT: {
            tell::store::initAgg(type, reinterpret_cast<int16_t*>(data));
        } break;

        case FieldType::INT: {
            tell::store::initAgg(type, reinterpret_cast<int32_t*>(data));
        } break;

        case FieldType::BIGINT: {
            tell::store::initAgg(type, reinterpret_cast<int64_t*>(data));
        } break;

        case FieldType::FLOAT: {
            tell::store::initAgg(type, reinterpret_cast<float*>(data));
        } break;

        case FieldType::DOUBLE: {
            tell::store::initAgg(type, reinterpret_cast<double*>(data));
        } break;

        case FieldType::TEXT:
        case FieldType::BLOB: {
            LOG_ASSERT(false, "Can not do this kind of aggregation on non-numeric types");
        } break;

        default: {
            LOG_ASSERT(false, "Unknown type");
        }
        }
    }

    FieldType aggType(AggregationType type) const {
        switch (type) {
        case AggregationType::MIN:
        case AggregationType::MAX: {
            return mType;
        } break;

        case AggregationType::SUM: {
            auto isFloat = (mType == FieldType::FLOAT) || (mType == FieldType::DOUBLE);
            if (isFloat) {
                return FieldType::DOUBLE;
            } else {
                return FieldType::BIGINT;
            }
        } break;

        case AggregationType::CNT: {
            return FieldType::BIGINT;
        } break;

        default: {
            LOG_ASSERT(false, "Unknown type");
            return FieldType::NOTYPE;
        }
        }
    }
};

class Field : public FieldBase {
    friend class Record;
private:
    crossbow::string mName;
    bool mNotNull = false;
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

    bool isNotNull() const {
        return mNotNull;
    }
};

/**
* This class is used to create and parse a table schema.
*
* The schema orders the fields in a way, that all fixed size columns
* come first. This allows for faster column lookup if a fixed size
* column is needed.
*
* The format is like follows:
* - 2 bytes: number of columns
* - 1 byte: Type of the table
* - 1 byte: Padding
* - For each column:
*   - 2 bytes: type of column
*   - 1 byte: 1 if it is non-nullable, 0 otherwise
*   - 1 byte: padding
*   - The name of the column, which is a string formatted like this:
*     - 4 bytes: size of the string in bytes (not in characters! this
*       string is not Unicode aware)
*     - The string
*     - alignment to 4 bytes
* - 2 bytes: Number of indexes
* - For each index:
*   - 2 bytes: number of columns to index
*   - 1 byte: boolean, set to true iff the UNIQUE constraint is set
*   - 1 byte: padding
*   - 2 bytes: length of name
*   - string - aligned to 2
*   - For each column:
*       - 2 byte: column id
*/
class Schema {
public:
    using id_t = uint16_t;
    using IndexMap = std::unordered_map<crossbow::string, std::pair<bool, std::vector<id_t>>>;
private:
    TableType mType = TableType::UNKNOWN;
    size_t mNullFields = 0;
    std::vector<Field> mFixedSizeFields;
    std::vector<Field> mVarSizeFields;
    IndexMap mIndexes;
public:
    Schema() = default;

    Schema(TableType type)
            : mType(type) {
    }

    Schema(const Schema&) = default;
    Schema(Schema&& schema) = default;

    Schema& operator=(Schema&&) = default;
    Schema& operator=(const Schema&) = default;

    bool addField(FieldType type, const crossbow::string& name, bool notNull);
    template<class Name, class Fields>
    void addIndex(Name&& name, Fields&& fields) {
        mIndexes.emplace(std::forward<Name>(name), std::forward<Fields>(fields));
    }
    template<class Name>
    void addIndex(Name&& name, std::initializer_list<id_t> fields) {
        mIndexes.emplace(std::forward<Name>(name), fields);
    }

    TableType type() const {
        return mType;
    }

    bool allNotNull() const {
        return (mNullFields == 0);
    }

    size_t nullFields() const {
        return mNullFields;
    }

    const std::vector<Field>& fixedSizeFields() const {
        return mFixedSizeFields;
    }

    const std::vector<Field>& varSizeFields() const {
        return mVarSizeFields;
    }

    const IndexMap& indexes() const {
        return mIndexes;
    }

    id_t idOf(const crossbow::string& name) const {
        id_t res = 0;
        for (const auto& field : mFixedSizeFields) {
            if (field.name() == name) {
                return res;
            }
            ++res;
        }
        for (const auto& field : mVarSizeFields) {
            if (field.name() == name) {
                return res;
            }
            ++res;
        }
        throw std::range_error("field does not exist");
    }

    const Field& getFieldFromName(const crossbow::string& name) const {
        for (const auto& field : mFixedSizeFields) {
            if (field.name() == name) {
                return field;
            }
        }
        for (const auto& field : mVarSizeFields) {
            if (field.name() == name) {
                return field;
            }
        }
        throw std::range_error("field does not exist");
    }

public: // Serialization
    static Schema deserialize(crossbow::buffer_reader& reader);
    size_t serializedLength() const;
    void serialize(crossbow::buffer_writer& writer) const;
};

struct FieldMetaData {
    FieldMetaData(const Field& _field, uint32_t _offset, uint16_t _nullIdx)
            : field(_field),
              offset(_offset),
              nullIdx(_nullIdx) {
    }

    Field field;
    uint32_t offset;
    uint16_t nullIdx;
};

/**
* This class implements the physical representation of a tuple.
* Note that this class does not handle multiple versions for
* snapshot isolation, since this has to be handled in the
* table itself (it depends on the approach).
*
* The format look as follows (in the following order):
*  - NULL bytevector containing a byte for every column that might be null if it is null (set to 1) or not (set to 0)
*    This is omitted, if all columns are declared as NOT NULL
*  - A padding to the next multiple of 8 byte
*  - The row Data of all fixed size fields
*  In case the schema also contains variable size fields
*  - A padding to the next multiple of 4 byte
*  - A uint32_t array of size #variable-size-fields + 1 containing the offsets into the heap where the data for the
*    field is stored. The offset is calculated from the start of the record and the last offset contains points to the
*    end of the variable heap.
*  - The variable size heap storing the variable sized data
*/
class Record {
public:
    using id_t = Schema::id_t;
private:
    Schema mSchema;
    std::unordered_map<crossbow::string, id_t> mIdMap;
    std::vector<FieldMetaData> mFieldMetaData;
    uint32_t mStaticSize;
public:
    Record();

    Record(Schema schema);

    const Schema& schema() const {
        return mSchema;
    }

    size_t sizeOfTuple(const GenericTuple& tuple) const;

    size_t sizeOfTuple(const char* ptr) const;

    size_t headerSize() const {
        return mSchema.nullFields();
    }

    /**
     * @brief The combined size of all fields
     *
     * This includes the header, fixed size fields and variable size offsets.
     */
    uint32_t staticSize() const {
        return mStaticSize;
    }

    bool idOf(const crossbow::string& name, id_t& result) const;

    const char* data(const char* ptr, id_t id, bool& isNull, FieldType* type = nullptr) const;

    bool create(char* result, const GenericTuple& tuple, uint32_t recSize) const;
    char* create(const GenericTuple& tuple, size_t& size) const;

    size_t fieldCount() const {
        return mFieldMetaData.size();
    }

    size_t fixedSizeFieldCount() const {
        return mSchema.fixedSizeFields().size();
    }

    size_t varSizeFieldCount() const {
        return mSchema.varSizeFields().size();
    }

    const FieldMetaData& getFieldMeta(id_t id) const {
        return mFieldMetaData.at(id);
    }

    bool allNotNull() const {
        return mSchema.allNotNull();
    }

    bool isFieldNull(const char* ptr, id_t id) const {
        LOG_ASSERT(id < mSchema.nullFields(), "Trying to get null status from a non-NULL field");
        return ptr[id];
    }

    void setFieldNull(char* ptr, Record::id_t id, bool isNull) const {
        LOG_ASSERT(id < mSchema.nullFields(), "Trying to set a null field on non-NULL field");
        ptr[id] = (isNull ? 1 : 0);
    }
};

} // namespace store
} // namespace tell
