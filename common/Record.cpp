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
#include <tellstore/Record.hpp>

#include <crossbow/alignment.hpp>
#include <crossbow/enum_underlying.hpp>

namespace tell {
namespace store {


Field::Field(Field&& other)
    : FieldBase(other.mType)
    , mName(std::move(other.mName))
    , mNotNull(other.mNotNull)
    , mData(other.mData)
{}

Field::Field(const Field& other)
    : FieldBase(other.mType)
    , mName(other.mName)
    , mNotNull(other.mNotNull)
    , mData(other.mData)
{}

Field& Field::operator=(Field&& other)
{
    mType = other.mType;
    mName = std::move(other.mName);
    mNotNull = other.mNotNull;
    mData = other.mData;
    return *this;
}

Field& Field::operator=(const Field& other)
{
    mType = other.mType;
    mName = other.mName;
    mNotNull = other.mNotNull;
    mData = other.mData;
    return *this;
}


size_t Field::defaultSize() const {
    if (isFixedSized()) return staticSize();
    switch (mType) {
        case FieldType::NULLTYPE:
            LOG_ASSERT(false, "NULLTYPE is not appropriate to use in a schema");
            return 0;
        case FieldType::TEXT:
            return sizeof(uint32_t);
        case FieldType::BLOB:
            return sizeof(uint32_t);
        case FieldType::NOTYPE:
            LOG_ASSERT(false, "One should never use a field of type NOTYPE");
            return std::numeric_limits<size_t>::max();
        default:
            LOG_ASSERT(false, "Unknown type");
            return 0;
    }
}

size_t Field::sizeOf(const boost::any& value) const {
    if (isFixedSized()) return staticSize();
    switch (mType) {
        case FieldType::NULLTYPE:
            LOG_ASSERT(false, "NULLTYPE is not appropriate to use in a schema");
            return 0;
        case FieldType::TEXT:
        case FieldType::BLOB:
            return sizeof(uint32_t) + boost::any_cast<crossbow::string>(value).size();
        case FieldType::NOTYPE:
            LOG_ASSERT(false, "One should never use a field of type NOTYPE");
            return std::numeric_limits<size_t>::max();
        default:
            LOG_ASSERT(false, "Unknown type");
            return 0;
    }
}

bool Schema::addField(FieldType type, const crossbow::string& name, bool notNull) {
    if (name.size() > std::numeric_limits<uint16_t>::max()) {
        LOG_ERROR("Field name with %d bytes are not supported", name.size());
        return false;
    }
    if (mFixedSizeFields.size() + mVarSizeFields.size() + 1 > std::numeric_limits<uint16_t>::max()) {
        LOG_ERROR("%d is the maximum number of columns in a table", std::numeric_limits<uint16_t>::max());
        return false;
    }
    bool res = true;
    for (auto iter = mFixedSizeFields.begin(); iter != mFixedSizeFields.end() && res; ++iter) {
        res = iter->name() != name;
    }
    for (auto iter = mVarSizeFields.begin(); iter != mVarSizeFields.end() && res; ++iter) {
        res = iter->name() != name;
    }
    if (!res) {
        LOG_TRACE("Tried to insert an already existing field: %s", name);
        return res;
    }
    mAllNotNull &= notNull;
    Field f(type, name, notNull);
    if (f.isFixedSized()) {
        mFixedSizeFields.emplace_back(f);
    } else {
        mVarSizeFields.emplace_back(f);
    }
    return true;
}

void Schema::addIndexes(const std::vector<std::vector<crossbow::string>>& idxs)
{
    std::vector<std::vector<id_t>> indexes;
    indexes.resize(idxs.size());
    for (decltype(idxs.size()) i = 0; i < idxs.size(); ++i) {
        for (decltype(idxs[i].size()) j = 0; j < idxs[i].size(); ++j) {
            indexes[i].resize(idxs[i][j].size());
        }
    }
    id_t numFields = id_t(mFixedSizeFields.size() + mVarSizeFields.size());
    // this is a simple nested loop join
    for (id_t id = 0; id < numFields; ++id) {
        auto& str = id < mFixedSizeFields.size() ?
            mFixedSizeFields[id].name()
            : mVarSizeFields[id - mFixedSizeFields.size()].name();
        for (decltype(idxs.size()) i = 0; i < idxs.size(); ++i) {
            for (decltype(idxs[i].size()) j = 0; j < idxs[i].size(); ++i) {
                if (idxs[i][j] == str) {
                    indexes[i][j] = id;
                }
            }
        }
    }
    mIndexes = std::move(indexes);
}

Record::Record(Schema schema)
        : mSchema(std::move(schema)),
          mFieldMetaData(mSchema.fixedSizeFields().size() + mSchema.varSizeFields().size()) {
    int32_t currOffset = headerSize();

    size_t id = 0;
    for (const auto& field : mSchema.fixedSizeFields()) {
        mIdMap.insert(std::make_pair(field.name(), id));
        mFieldMetaData[id++] = std::make_pair(field, currOffset);
        currOffset += field.staticSize();
    }
    for (const auto& field : mSchema.varSizeFields()) {
        mIdMap.insert(std::make_pair(field.name(), id));
        mFieldMetaData[id++] = std::make_pair(field, currOffset);
        // make sure, that all others are set to min
        currOffset = std::numeric_limits<int32_t>::min();
    }
}

size_t Schema::serializedLength() const
{
    size_t res = sizeof(uint32_t);
    for (auto& f : mFixedSizeFields) {
        res += 2 * sizeof(uint16_t) + sizeof(uint32_t);
        res += f.name().size();
        res = crossbow::align(res, sizeof(uint32_t));
    }
    for (auto& f : mVarSizeFields) {
        res += 2 * sizeof(uint16_t) + sizeof(uint32_t);
        res += f.name().size();
        res = crossbow::align(res, sizeof(uint32_t));
    }
    res += sizeof(uint16_t);
    for (auto& idx : mIndexes) {
        res += sizeof(uint16_t);
        res += idx.size() * sizeof(id_t);
    }
    return res;
}

void Schema::serialize(crossbow::buffer_writer& writer) const
{
    writer.write<uint16_t>(mFixedSizeFields.size() + mVarSizeFields.size());
    writer.write<TableType>(mType);
    writer.write<uint8_t>(0u);
    auto writeField = [&writer](const Field& f) {
        writer.write<FieldType>(f.type());
        writer.write<uint8_t>(f.isNotNull() ? 1u : 0u);
        writer.write<uint8_t>(0u);
        writer.write<uint32_t>(f.name().size());
        writer.write(f.name().c_str(), f.name().size());
        writer.align(sizeof(uint32_t));
    };
    for (auto& f : mFixedSizeFields) {
        writeField(f);
    }
    for (auto& f : mVarSizeFields) {
        writeField(f);
    }
    writer.write<uint16_t>(mIndexes.size());
    for (auto& idx : mIndexes) {
        writer.write<uint16_t>(idx.size());
        for (auto id : idx) {
            writer.write<id_t>(id);
        }
    }
}

Schema Schema::deserialize(crossbow::buffer_reader& reader)
{
    Schema res;
    auto numColumns = reader.read<uint16_t>();
    res.mType = reader.read<TableType>();
    reader.advance(sizeof(uint8_t));
    for (uint16_t i = 0; i < numColumns; ++i) {
        auto ftype = reader.read<FieldType>();
        bool isNotNull = (reader.read<uint8_t>() != 0x0u);
        reader.advance(sizeof(uint8_t));
        auto nameLen = reader.read<uint32_t>();
        crossbow::string name(reader.data(), nameLen);
        reader.advance(nameLen);
        reader.align(sizeof(uint32_t));
        Field field(ftype, name, isNotNull);
        if (field.isFixedSized()) {
            res.mFixedSizeFields.emplace_back(std::move(field));
        } else {
            res.mVarSizeFields.emplace_back(std::move(field));
        }
    }
    auto numIndexes = reader.read<uint16_t>();
    for (uint16_t i = 0; i < numIndexes; ++i) {
        auto numCols = reader.read<uint16_t>();
        std::vector<id_t> ids;
        for (uint16_t i = 0; i < numCols; ++i) {
            ids.push_back(reader.read<id_t>());
        }
        res.mIndexes.emplace_back(std::move(ids));
    }
    return res;
}

size_t Record::sizeOfTuple(const GenericTuple& tuple) const
{
    auto result = headerSize();
    for (auto& f : mFieldMetaData) {
        auto& field = f.first;
        if (field.isFixedSized()) {
            result += field.staticSize();
            // we will need this space anyway
            // no matter what tuple contains
            continue;
        }
        const auto& name = field.name();
        auto iter = tuple.find(name);
        if (iter == tuple.end()) {
            result += field.defaultSize();
        } else {
            result += field.sizeOf(iter->second);
        }
    }
    // we have to make sure that the size of a tuple is 8 byte aligned
    return crossbow::align(result, 8);
}

size_t Record::sizeOfTuple(const char* ptr) const {
    // If the schema has no variable sized fields then the size of a tuple is the offset of the last element plus its
    // size else we have to calculate the variable sized fields
    if (mFieldMetaData.empty()) {
        return 0;
    }

    size_t pos = 0;
    if (mSchema.varSizeFields().empty()) {
        auto& f = mFieldMetaData.back();
        LOG_ASSERT(f.first.isFixedSized(), "Element must be fixed size in Schema with no variable sized fields");
        pos = f.second + f.first.staticSize();
    } else {
        auto baseId = mSchema.fixedSizeFields().size();
        auto offset = mFieldMetaData[baseId].second;
        LOG_ASSERT(offset >= 0, "Offset for first variable length field is smaller than 0");
        pos = offset;

        for (; baseId < mFieldMetaData.size(); ++baseId) {
            // we know, that now all fields are variable length - that means the first four bytes are always the field
            // size
            pos += *reinterpret_cast<const uint32_t* const>(ptr + pos) + sizeof(uint32_t);
        }
    }
    // we have to make sure that the size of a tuple is 8 byte aligned
    return crossbow::align(pos, 8);
}

size_t Record::headerSize() const {
    if (mSchema.allNotNull()) {
        return 0u;
    }

    size_t result = (mFieldMetaData.size() + 7) / 8;
    return crossbow::align(result, 8);
}

size_t Record::minimumSize() const {
    if (mFieldMetaData.empty()) {
        return 0;
    }

    size_t length = 0;
    if (mSchema.varSizeFields().empty()) {
        auto& f = mFieldMetaData.back();
        LOG_ASSERT(f.first.isFixedSized(), "Element must be fixed size in Schema with no variable sized fields");
        length = f.second + f.first.staticSize();
    } else {
        auto baseId = mSchema.fixedSizeFields().size();
        auto offset = mFieldMetaData[baseId].second;
        LOG_ASSERT(offset >= 0, "Offset for first variable length field is smaller than 0");

        // Variable sized fields are always at least 4 bytes to indicate a NULL length
        length = offset + (mSchema.varSizeFields().size() * sizeof(uint32_t));
    }

    // Align the length to 8 bytes
    return crossbow::align(length, 8);
}


char* Record::create(const GenericTuple& tuple, size_t& size) const {
    uint32_t recSize = uint32_t(sizeOfTuple(tuple));
    size = size_t(recSize);
    std::unique_ptr<char[]> result(new char[recSize]);
    if (!create(result.get(), tuple, recSize)) {
        return nullptr;
    }
    return result.release();
}

bool Record::create(char* result, const GenericTuple& tuple, uint32_t recSize) const {
    LOG_ASSERT(recSize == sizeOfTuple(tuple), "Size has to be the actual tuple size");
    auto headerOffset = headerSize();
    memset(result, 0, headerOffset);
    char* current = result + headerOffset;
    for (id_t id = 0; id < mFieldMetaData.size(); ++id) {
        auto& f = mFieldMetaData[id];
        const auto& name = f.first.name();
        LOG_ASSERT(f.second == std::numeric_limits<int32_t>::min() || current == (result + f.second),
                "Trying to write fixed size field to wrong offset");

        // first we need to check whether the value for this field is given
        auto iter = tuple.find(name);
        if (iter == tuple.end()) {
            // No value is given so set the field to NULL if the field can be NULL (abort when it must not be NULL)
            if (f.first.isNotNull()) {
                return false;
            }

            // In this case we set the field to NULL
            setFieldNull(result, id, true);

            // Write a string of \0 bytes as a default value for fields if the field is null. This is for performance
            // reason as any fixed size field has a constant offset.
            auto fieldLength = f.first.defaultSize();
            memset(current, 0, fieldLength);
            current += fieldLength;
        } else {
            // we just need to copy the value to the correct offset.
            switch (f.first.type()) {
            case FieldType::NOTYPE: {
                LOG_ASSERT(false, "Try to write something with no type");
                return false;
            }
            case FieldType::NULLTYPE: {
                LOG_ASSERT(false, "NULLTYPE is not allowed here");
                return false;
            }
            case FieldType::SMALLINT: {
                memcpy(current, boost::any_cast<int16_t>(&(iter->second)), sizeof(int16_t));
                current += sizeof(int16_t);
            } break;
            case FieldType::INT: {
                memcpy(current, boost::any_cast<int32_t>(&(iter->second)), sizeof(int32_t));
                current += sizeof(int32_t);
            } break;
            case FieldType::BIGINT: {
                memcpy(current, boost::any_cast<int64_t>(&(iter->second)), sizeof(int64_t));
                current += sizeof(int64_t);
            } break;
            case FieldType::FLOAT: {
                memcpy(current, boost::any_cast<float>(&(iter->second)), sizeof(float));
                current += sizeof(float);
            } break;
            case FieldType::DOUBLE: {
                memcpy(current, boost::any_cast<double>(&(iter->second)), sizeof(double));
                current += sizeof(double);
            } break;
            case FieldType::TEXT:
            case FieldType::BLOB: {
                const crossbow::string& str = *boost::any_cast<crossbow::string>(&(iter->second));
                uint32_t len = uint32_t(str.size());
                memcpy(current, &len, sizeof(len));
                current += sizeof(uint32_t);
                memcpy(current, str.c_str(), len);
                current += len;
            } break;

            default: {
                LOG_ASSERT(false, "Unknown field type");
                return false;
            }
            }
        }
    }
    return true;
}

const char* Record::data(const char* const ptr, Record::id_t id, bool& isNull, FieldType* type /* = nullptr*/) const {
    if (id >= mFieldMetaData.size()) {
        LOG_ASSERT(false, "Tried to get nonexistent id");
        return nullptr;
    }
    const auto& p = mFieldMetaData[id];
    if (type != nullptr) {
        *type = p.first.type();
    }
    isNull = isFieldNull(ptr, id);
    if (p.second < 0) {
        // we need to calc the position
        auto baseId = mSchema.fixedSizeFields().size();
        auto pos = mFieldMetaData[baseId].second;
        LOG_ASSERT(pos >= 0, "Offset for first variable length field is smaller than 0");
        for (; baseId < id; ++baseId) {
            // we know, that now all fields are variable length - that means the first four bytes are always the field
            // size
            pos += *reinterpret_cast<const uint32_t* const>(ptr + pos) + sizeof(uint32_t);
        }
        return ptr + pos;
    } else {
        return ptr + p.second;
    }
}

bool Record::idOf(const crossbow::string& name, id_t& result) const {
    auto iter = mIdMap.find(name);
    if (iter == mIdMap.end()) return false;
    result = iter->second;
    return true;
}

char* Record::data(char* const ptr, Record::id_t id, bool& isNull, FieldType* type /* = nullptr */) {
    auto res = const_cast<const Record*>(this)->data(ptr, id, isNull, type);
    return const_cast<char*>(res);
}

Field Record::getField(char* const ptr, id_t id) {
    if (id >= mFieldMetaData.size()) {
        LOG_ERROR("Tried to read non-existent field");
        assert(false);
        return Field();
    }
    bool isNull;
    FieldType type;
    auto dPtr = data(ptr, id, isNull, &type);
    if (isNull)
        return Field();
    auto res = mFieldMetaData[id].first;
    res.mData = dPtr;
    return res;
}

Field Record::getField(char* const ptr, const crossbow::string& name)
{
    id_t i;
    if (!idOf(name, i)) {
        LOG_ERROR("Unknown Field %s", name);
        return Field();
    }
    return getField(ptr, i);
}

bool Record::isFieldNull(const char* ptr, Record::id_t id) const {
    using uchar = unsigned char;

    if (mSchema.allNotNull()) {
        return false;
    }

    // TODO: Check whether the compiler optimizes this correctly - otherwise this might be inefficient (but more readable)
    auto bitmap = *reinterpret_cast<const uchar*>(ptr + id / 8);
    auto mask = uchar(0x1 << (id % 8));
    return (bitmap & mask) == 0;
}

void Record::setFieldNull(char* ptr, Record::id_t id, bool isNull) const {
    using uchar = unsigned char;

    LOG_ASSERT(!mSchema.allNotNull(), "Trying to set a null field on non-NULL schema")

    auto& bitmap = *reinterpret_cast<uchar*>(ptr + id / 8);
    auto mask = uchar(0x1 << (id % 8));

    if (isNull) {
        bitmap |= mask;
    } else {
        bitmap &= ~mask;
    }
}

} // namespace store
} // namespace tell
