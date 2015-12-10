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
#pragma once

#include <tellstore/Record.hpp>

#include <crossbow/non_copyable.hpp>
#include <crossbow/string.hpp>

#include <boost/any.hpp>

#include <cstdint>
#include <memory>
#include <stdexcept>

namespace crossbow {
class buffer_reader;
} // namespace crossbow

namespace tell {
namespace store {

class Tuple final : crossbow::non_copyable, crossbow::non_movable {
public: // Construction
    void* operator new(size_t size, uint32_t dataLen);

    void* operator new[](size_t size) = delete;

    void operator delete(void* ptr);

    void operator delete[](void* ptr) = delete;

public: // Serialization
    static std::unique_ptr<Tuple> deserialize(crossbow::buffer_reader& reader);

public:
    const char* data() const {
        return reinterpret_cast<const char*>(this) + sizeof(Tuple);
    }

    uint64_t version() const {
        return mVersion;
    }

    bool isNewest() const {
        return mIsNewest;
    }

    size_t size() const {
        return mSize;
    }

private:
    Tuple(uint64_t version, bool isNewest, uint32_t size)
            : mVersion(version),
              mIsNewest(isNewest),
              mSize(size) {
    }

    uint64_t mVersion;
    bool mIsNewest;
    uint32_t mSize;
};

class Table {
public:
    Table()
            : mTableId(0x0u) {
    }

    Table(uint64_t tableId, Schema schema)
            : mTableId(tableId),
              mRecord(std::move(schema)) {
    }

    uint64_t tableId() const {
        return mTableId;
    }

    const Record& record() const {
        return mRecord;
    }

    TableType tableType() const {
        return mRecord.schema().type();
    }

    template <typename T>
    T field(const crossbow::string& name, const char* data) const;

    GenericTuple toGenericTuple(const char* data) const;

private:
    uint64_t mTableId;
    Record mRecord;
};

template <typename T>
T Table::field(const crossbow::string& name, const char* data) const {
    Record::id_t id;
    if (!mRecord.idOf(name, id)) {
        throw std::logic_error("Field not found");
    }

    bool isNull;
    FieldType type;
    auto field = mRecord.data(data, id, isNull, &type);
    if (isNull) {
        throw std::logic_error("Field is null");
    }

    boost::any value;
    switch (type) {

    case FieldType::SMALLINT: {
        value = *reinterpret_cast<const int16_t*>(field);
    } break;

    case FieldType::INT: {
        value = *reinterpret_cast<const int32_t*>(field);
    } break;

    case FieldType::BIGINT: {
        value = *reinterpret_cast<const int64_t*>(field);
    } break;

    case FieldType::FLOAT: {
        value = *reinterpret_cast<const float*>(field);
    } break;

    case FieldType::DOUBLE: {
        value = *reinterpret_cast<const double*>(field);
    } break;

    case FieldType::TEXT:
    case FieldType::BLOB: {
        auto offsetData = reinterpret_cast<const uint32_t*>(field);
        auto offset = offsetData[0];
        auto length = offsetData[1] - offset;
        value = crossbow::string(data + offset, length);
    } break;

    default: {
        throw std::logic_error("Invalid field type");
    } break;
    }

    return boost::any_cast<T>(value);
}

} // namespace store
} // namespace tell
