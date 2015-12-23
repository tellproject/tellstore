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
{}

Field::Field(const Field& other)
    : FieldBase(other.mType)
    , mName(other.mName)
    , mNotNull(other.mNotNull)
{}

Field& Field::operator=(Field&& other)
{
    mType = other.mType;
    mName = std::move(other.mName);
    mNotNull = other.mNotNull;
    return *this;
}

Field& Field::operator=(const Field& other)
{
    mType = other.mType;
    mName = other.mName;
    mNotNull = other.mNotNull;
    return *this;
}

bool Schema::addField(FieldType type, const crossbow::string& name, bool notNull) {
    if (!mIndexes.empty()) {
        LOG_ERROR("Can not add more fields after adding indexes");
        return false;
    }
    if (name.size() > std::numeric_limits<uint16_t>::max()) {
        LOG_ERROR("Field name with %d bytes are not supported", name.size());
        return false;
    }
    if (mFixedSizeFields.size() + mVarSizeFields.size() + 1 > std::numeric_limits<uint16_t>::max()) {
        LOG_ERROR("%d is the maximum number of columns in a table", std::numeric_limits<uint16_t>::max());
        return false;
    }

    Field f(type, name, notNull);
    auto alignment = f.alignOf();
    bool res = true;
    auto insertPos = mFixedSizeFields.begin();
    for (auto iter = mFixedSizeFields.begin(); iter != mFixedSizeFields.end() && res; ++iter) {
        if (alignment <= iter->alignOf()) {
            insertPos = std::next(iter);
        }
        res = iter->name() != name;
    }
    for (auto iter = mVarSizeFields.begin(); iter != mVarSizeFields.end() && res; ++iter) {
        res = iter->name() != name;
    }
    if (!res) {
        LOG_TRACE("Tried to insert an already existing field: %s", name);
        return res;
    }
    if (!notNull) {
        ++mNullFields;
    }
    if (f.isFixedSized()) {
        mFixedSizeFields.emplace(insertPos, f);
    } else {
        mVarSizeFields.emplace_back(f);
    }
    return true;
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
        res += 3 * sizeof(uint16_t);
        auto strLen = idx.first.size();
        res += strLen + (strLen % 2);
        res += sizeof(id_t) * idx.second.second.size();
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
        auto strLen = idx.first.size();
        writer.write<uint16_t>(idx.second.second.size());
        writer.write<uint8_t>(idx.second.first);
        writer.write<uint8_t>(0);
        writer.write<uint16_t>(strLen);
        writer.write(idx.first.data(), strLen + (strLen % 2));
        for (auto id : idx.second.second) {
            writer.write<decltype(id)>(id);
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
        bool notNull = (reader.read<uint8_t>() != 0x0u);
        reader.advance(sizeof(uint8_t));
        auto nameLen = reader.read<uint32_t>();
        crossbow::string name(reader.data(), nameLen);
        reader.advance(nameLen);
        reader.align(sizeof(uint32_t));
        if (!notNull) {
            ++res.mNullFields;
        }
        Field field(ftype, name, notNull);
        if (field.isFixedSized()) {
            res.mFixedSizeFields.emplace_back(std::move(field));
        } else {
            res.mVarSizeFields.emplace_back(std::move(field));
        }
    }
    auto numIndexes = reader.read<uint16_t>();
    for (uint16_t i = 0; i < numIndexes; ++i) {
        uint16_t numFields = reader.read<uint16_t>();
        bool isUnique = reader.read<uint8_t>();
        reader.read<uint8_t>(); // padding
        uint16_t nameSize = reader.read<uint16_t>();
        const char* name = reader.data();
        reader.advance(nameSize + (nameSize % 2));
        std::vector<id_t> fields;
        for (uint16_t j = 0; j < numFields; ++j) {
            fields.push_back(reader.read<uint16_t>());
        }
        res.mIndexes.emplace(crossbow::string(name, nameSize), std::make_pair(isUnique, std::move(fields)));
    }
    return res;
}

Record::Record()
        : mStaticSize(0u) {
}

Record::Record(Schema schema)
        : mSchema(std::move(schema)) {
    auto count = mSchema.fixedSizeFields().size() + mSchema.varSizeFields().size();
    mIdMap.reserve(count);
    mFieldMetaData.reserve(count);

    mStaticSize = crossbow::align(mSchema.nullFields(), 8u);

#ifndef NDEBUG
    auto lastAlignment = std::numeric_limits<size_t>::max();
    for (const auto& field : mSchema.fixedSizeFields()) {
        auto alignment = field.alignOf();
        LOG_ASSERT(lastAlignment >= alignment, "Alignment not in descending order");
        lastAlignment = alignment;
    }
#endif

    size_t idx = 0;
    uint16_t nullIdx = 0;
    for (const auto& field : mSchema.fixedSizeFields()) {
        mIdMap.insert(std::make_pair(field.name(), idx));
        mFieldMetaData.emplace_back(field, mStaticSize, field.isNotNull() ? 0 : nullIdx++);
        mStaticSize += field.staticSize();
        ++idx;
    }

    if (!mSchema.varSizeFields().empty()) {
        mStaticSize = crossbow::align(mStaticSize, 4u);
        for (const auto& field : mSchema.varSizeFields()) {
            mIdMap.insert(std::make_pair(field.name(), idx));
            mFieldMetaData.emplace_back(field, mStaticSize, field.isNotNull() ? 0 : nullIdx++);
            mStaticSize += sizeof(uint32_t);
            ++idx;
        }
        // Allocate an additional entry for the last offset
        mStaticSize += sizeof(uint32_t);
    }
}

size_t Record::sizeOfTuple(const GenericTuple& tuple) const {
    auto result = mStaticSize;

    // Iterate over all variable sized fields
    for (auto i = std::next(mFieldMetaData.begin(), mSchema.fixedSizeFields().size()); i != mFieldMetaData.end(); ++i) {
        auto& field = i->field;

        auto iter = tuple.find(field.name());
        if (iter != tuple.end()) {
            result += boost::any_cast<const crossbow::string&>(iter->second).size();
        }
    }
    return result;
}

size_t Record::sizeOfTuple(const char* ptr) const {
    if (mSchema.varSizeFields().empty()) {
        return mStaticSize;
    }

    // In case the record has variable sized fields the total size can be calculated by getting the end offset of
    // the variable sized field
    return *reinterpret_cast<const uint32_t*>(ptr + mStaticSize - sizeof(uint32_t));
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
    memset(result, 0, mSchema.nullFields());
    auto varHeapOffset = mStaticSize;
    for (id_t id = 0; id < mFieldMetaData.size(); ++id) {
        auto& f = mFieldMetaData[id];
        auto& field = f.field;
        auto current = result + f.offset;

        // first we need to check whether the value for this field is given
        auto iter = tuple.find(field.name());
        if (iter == tuple.end()) {
            // No value is given so set the field to NULL if the field can be NULL (abort when it must not be NULL)
            if (field.isNotNull()) {
                return false;
            }

            // In this case we set the field to NULL
            result[f.nullIdx] = 1;

            if (field.isFixedSized()) {
                // Write a string of \0 bytes as a default value for fields if the field is null. This is for
                // performance reason as any fixed size field has a constant offset.
                memset(current, 0, field.staticSize());
            } else {
                // If this is a variable sized field we have to set the heap offset
                *reinterpret_cast<uint32_t*>(current) = varHeapOffset;
            }
        } else {
            // we just need to copy the value to the correct offset.
            switch (field.type()) {
            case FieldType::NOTYPE: {
                LOG_ASSERT(false, "Try to write something with no type");
                return false;
            }
            case FieldType::NULLTYPE: {
                LOG_ASSERT(false, "NULLTYPE is not allowed here");
                return false;
            }
            case FieldType::SMALLINT: {
                LOG_ASSERT(reinterpret_cast<uintptr_t>(current) % alignof(int16_t) == 0u,
                        "Pointer to field must be aligned");
                memcpy(current, boost::any_cast<int16_t>(&(iter->second)), sizeof(int16_t));
            } break;
            case FieldType::INT: {
                LOG_ASSERT(reinterpret_cast<uintptr_t>(current) % alignof(int32_t) == 0u,
                        "Pointer to field must be aligned");
                memcpy(current, boost::any_cast<int32_t>(&(iter->second)), sizeof(int32_t));
            } break;
            case FieldType::BIGINT: {
                LOG_ASSERT(reinterpret_cast<uintptr_t>(current) % alignof(int64_t) == 0u,
                        "Pointer to field must be aligned");
                memcpy(current, boost::any_cast<int64_t>(&(iter->second)), sizeof(int64_t));
            } break;
            case FieldType::FLOAT: {
                LOG_ASSERT(reinterpret_cast<uintptr_t>(current) % alignof(float) == 0u,
                        "Pointer to field must be aligned");
                memcpy(current, boost::any_cast<float>(&(iter->second)), sizeof(float));
            } break;
            case FieldType::DOUBLE: {
                LOG_ASSERT(reinterpret_cast<uintptr_t>(current) % alignof(double) == 0u,
                        "Pointer to field must be aligned");
                memcpy(current, boost::any_cast<double>(&(iter->second)), sizeof(double));
            } break;
            case FieldType::TEXT:
            case FieldType::BLOB: {
                LOG_ASSERT(reinterpret_cast<uintptr_t>(current) % alignof(uint32_t) == 0u,
                        "Pointer to field must be aligned");
                *reinterpret_cast<uint32_t*>(current) = varHeapOffset;

                auto& data = *boost::any_cast<crossbow::string>(&(iter->second));
                memcpy(result + varHeapOffset, data.c_str(), data.size());
                varHeapOffset += data.size();
            } break;

            default: {
                LOG_ASSERT(false, "Unknown field type");
                return false;
            }
            }
        }
    }

    // Set the last offset to the end
    if (!mSchema.varSizeFields().empty()) {
        auto current = result + mStaticSize - sizeof(uint32_t);
        *reinterpret_cast<uint32_t*>(current) = varHeapOffset;
    }
    return true;
}

const char* Record::data(const char* ptr, Record::id_t id, bool& isNull, FieldType* type /* = nullptr*/) const {
    if (id >= mFieldMetaData.size()) {
        LOG_ASSERT(false, "Tried to get nonexistent id");
        return nullptr;
    }
    const auto& f = mFieldMetaData[id];
    auto& field = f.field;
    if (type != nullptr) {
        *type = field.type();
    }
    if (!field.isNotNull()) {
        isNull = isFieldNull(ptr, f.nullIdx);
    }
    return ptr + f.offset;
}

bool Record::idOf(const crossbow::string& name, id_t& result) const {
    auto iter = mIdMap.find(name);
    if (iter == mIdMap.end()) return false;
    result = iter->second;
    return true;
}

} // namespace store
} // namespace tell
