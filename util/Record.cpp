#include "Record.hpp"
#include "Logging.hpp"

namespace tell {
namespace store {
namespace impl {

size_t Field::staticSize() const {
    switch (mType) {
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
            LOG_DEBUG("Tried to get static size of TEXT Field, which does not have a static size");
            return std::numeric_limits<size_t>::max();
        case FieldType::BLOB:
            LOG_DEBUG("Tried to get static size of BLOB Field, which does not have a static size");
            return std::numeric_limits<size_t>::max();
        case FieldType::NOTYPE:
            assert(false);
            LOG_ERROR("One should never use a field of type NOTYPE");
            return std::numeric_limits<size_t>::max();
    }
}

bool Schema::addField(FieldType type, const crossbow::string &name, bool notNull) {
    if (name.size() > std::numeric_limits<uint16_t>::max()) {
        LOG_DEBUG("Field name with %d bytes are not supported", name.size());
        return false;
    }
    if (mFixedSizeFields.size() + mVarSizeFields.size() + 1 > std::numeric_limits<uint16_t>::max()) {
        LOG_ERROR("%d is the maximum number of columns in a table", std::numeric_limits<uint16_t>::max());
        return false;
    }
    bool res = true;
    for (auto iter = mFixedSizeFields.begin(); iter != mVarSizeFields.end() && res; ++iter) {
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
    Field f(type, name);
    if (f.isFixedSized()) {
        mFixedSizeFields.emplace_back(f);
    } else {
        mVarSizeFields.emplace_back(f);
    }
    return true;
}

size_t Schema::schemaSize() const {
    size_t res = 8;
    for (auto& field: mFixedSizeFields) {
        res += 4;
        res += field.name().size();
    }
    return res;
}

namespace {

inline char* serialize_field(const Field& field, char* ptr) {
    uint16_t fieldType = to_underlying(field.type());
    memcpy(ptr, &fieldType, sizeof(fieldType));
    ptr += sizeof(fieldType);
    const crossbow::string& name = field.name();
    uint16_t nameSize = uint16_t(name.size());
    memcpy(ptr, &nameSize, sizeof(nameSize));
    ptr += sizeof(nameSize);
    memcpy(ptr, name.data(), name.size());
    ptr += name.size();
    return ptr;
}

} // namespace {}

char *Schema::serialize(char *ptr) const {
    uint32_t sz = uint32_t(schemaSize());
    memcpy(ptr, &sz, sizeof(sz));
    ptr += sizeof(sz);
    uint16_t numColumns = uint16_t(mFixedSizeFields.size() + mVarSizeFields.size());
    memcpy(ptr, &numColumns, sizeof(numColumns));
    ptr += sizeof(numColumns);
    uint8_t allNotNull = uint8_t(mAllNotNull ? 1 : 0);
    memcpy(ptr, &allNotNull, sizeof(allNotNull));
    ptr += 2;
    for (auto& field : mFixedSizeFields) {
        ptr = serialize(ptr);
    }
    for (auto& field : mVarSizeFields) {
        ptr = serialize_field(field, ptr);
    }
    return ptr;
}

Schema::Schema(const char *ptr) {
    // we can ignore the size
    ptr += sizeof(uint32_t);
    uint16_t numColumns = *reinterpret_cast<const uint16_t*>(ptr);
    ptr += sizeof(numColumns);
    uint8_t allNotNull = *reinterpret_cast<const uint8_t*>(ptr);
    mAllNotNull = allNotNull > 0;
    ptr += 2;
    for (uint16_t i = 0; i < numColumns; ++i) {
        FieldType type = *reinterpret_cast<const FieldType*>(ptr);
        ptr += sizeof(type);
        uint16_t nameSize = *reinterpret_cast<const uint16_t*>(ptr);
        ptr += sizeof(nameSize);
        Field f(type, crossbow::string(ptr, nameSize));
        ptr += nameSize;
        if (f.isFixedSized()) {
            mFixedSizeFields.emplace_back(std::move(f));
        } else {
            mVarSizeFields.emplace_back(std::move(f));
        }
    }
}

Record::Record(const Schema &schema)
        : mSchema(schema),
          mFieldMetaData(schema.fixedSizeFields().size() + schema.varSizeFields().size())
{
    off_t headOffset = 4;
    {
        if (schema.varSizeFields().size() > 0) {
            headOffset += (mFieldMetaData.size() + 7)/8;
        }
        // Padding
        headOffset += 8 - (headOffset % 8);
    }
    off_t currOffset = headOffset;
    size_t id = 0;
    for (const auto& field : schema.fixedSizeFields()) {
        mIdMap.insert(std::make_pair(field.name(), id));
        mFieldMetaData[id++] = std::make_pair(field, currOffset);
        currOffset += field.staticSize();
    }
    for (const auto& field : schema.varSizeFields()) {
        mIdMap.insert(std::make_pair(field.name(), id));
        mFieldMetaData[id++] = std::make_pair(field, currOffset);
        // make sure, that all others are set to max
        currOffset = std::numeric_limits<off_t>::max();
    }
}

const char *Record::data(const char* const ptr, Record::id_t id, bool &isNull, FieldType* type /* = nullptr*/) const {
    if (id > mFieldMetaData.size()) {
        LOG_ERROR("Tried to get nonexistent id");
        assert(false);
        return nullptr;
    }
    uint32_t recSize = *reinterpret_cast<const uint32_t* const>(ptr);
    using uchar = unsigned char;
    isNull = false;
    if (!mSchema.allNotNull()) {
        // TODO: Check whether the compiler optimizes this correctly - otherwise this might be inefficient (but more readable)
        uchar bitmap = *reinterpret_cast<const uchar* const>(ptr + 4 + id/8);
        unsigned char pos = uchar(id % 8);
        isNull = (uchar(0x1 << pos) & bitmap) == 0;
    }
    const auto& p = mFieldMetaData[id];
    if (type != nullptr) {
        *type = p.first.type();
    }
    if (p.second > recSize) {
        // we need to calc the position
        auto baseId = mSchema.fixedSizeFields().size();
        off_t pos = mFieldMetaData[baseId].second;
        for (; baseId < id; ++baseId) {
            // we know, that now all fields are variable length - that means the first two bytes are always the
            // field size
            pos += *reinterpret_cast<const uint16_t* const>(ptr + pos) + sizeof(uint16_t);
        }
        return ptr + pos;
    } else {
        return ptr + p.second;
    }
}

bool Record::idOf(const crossbow::string &name, id_t& result) const {
    auto iter = mIdMap.find(name);
    if (iter == mIdMap.end()) return false;
    result = iter->second;
    return true;
}

bool Record::setNull(Record::id_t id, bool isNull) {
    return false;
}

char* Record::data(char *const ptr, Record::id_t id, bool &isNull, FieldType *type /* = nullptr */) {
    auto res = const_cast<const Record*>(this)->data(ptr, id, isNull, type);
    return const_cast<char*>(res);
}
} // namespace tell
} // namespace store
} // namespace impl
