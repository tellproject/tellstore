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
#include <tellstore/Table.hpp>

#include <crossbow/byte_buffer.hpp>
#include <crossbow/logger.hpp>

namespace tell {
namespace store {

void* Tuple::operator new(size_t size, uint32_t dataLen) {
    LOG_ASSERT(size == sizeof(Tuple), "Requested size does not match Tuple size");
    return ::malloc(size + dataLen);
}

void Tuple::operator delete(void* ptr) {
    ::free(ptr);
}

std::unique_ptr<Tuple> Tuple::deserialize(crossbow::buffer_reader& reader) {
    auto version = reader.read<uint64_t>();
    auto isNewest = reader.read<uint8_t>();
    reader.align(sizeof(uint32_t));
    auto size = reader.read<uint32_t>();

    std::unique_ptr<Tuple> tuple(new (size) Tuple(version, isNewest, size));
    if (tuple && size > 0) {
        memcpy(const_cast<char*>(tuple->data()), reader.read(size), size);
    }
    return tuple;
}

GenericTuple Table::toGenericTuple(const char* data) const {
    GenericTuple tuple;

    for (decltype(mRecord.fieldCount()) id = 0; id < mRecord.fieldCount(); ++id) {
        auto& metadata = mRecord.getFieldMeta(id);

        bool isNull;
        FieldType type;
        auto field = mRecord.data(data, id, isNull, &type);
        if (isNull) {
            continue;
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
            auto length = *reinterpret_cast<const int32_t*>(field);
            value = crossbow::string(field + sizeof(int32_t), length);
        } break;

        default: {
            throw std::logic_error("Invalid field type");
        } break;
        }

        tuple.emplace(metadata.first.name(), std::move(value));
    }

    return tuple;
}

} // namespace store
} // namespace tell
