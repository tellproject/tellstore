#include <tellstore/Record.hpp>

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
        LOG_DEBUG("Field name with %d bytes are not supported", name.size());
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

size_t Schema::schemaSize() const {
    size_t res = 8;
    // TODO Fix the alignment
    for (auto& field: mFixedSizeFields) {
        res += 5;
        res += field.name().size();
    }
    for (auto& field: mVarSizeFields) {
        res += 5;
        res += field.name().size();
    }
    return res;
}

namespace {

inline char* serialize_field(const Field& field, char* ptr) {
    uint16_t fieldType = crossbow::to_underlying(field.type());
    memcpy(ptr, &fieldType, sizeof(fieldType));
    ptr += sizeof(fieldType);
    bool isNotNull = field.isNotNull();
    memcpy(ptr, &isNotNull, sizeof(isNotNull));
    ptr += sizeof(isNotNull);
    const crossbow::string& name = field.name();
    uint16_t nameSize = uint16_t(name.size());
    memcpy(ptr, &nameSize, sizeof(nameSize));
    ptr += sizeof(nameSize);
    memcpy(ptr, name.data(), name.size());
    ptr += name.size();
    return ptr;
}

} // namespace {}

char* Schema::serialize(char* ptr) const {
    uint32_t sz = uint32_t(schemaSize());
    memcpy(ptr, &sz, sizeof(sz));
    ptr += sizeof(sz);
    uint16_t numColumns = uint16_t(mFixedSizeFields.size() + mVarSizeFields.size());
    memcpy(ptr, &numColumns, sizeof(numColumns));
    ptr += sizeof(numColumns);
    uint8_t type = static_cast<uint8_t>(mType);
    memcpy(ptr, &type, sizeof(type));
    ptr += sizeof(type);
    uint8_t allNotNull = uint8_t(mAllNotNull ? 1 : 0);
    memcpy(ptr, &allNotNull, sizeof(allNotNull));
    ptr += sizeof(allNotNull);
    for (auto& field : mFixedSizeFields) {
        ptr = serialize_field(field, ptr);
    }
    for (auto& field : mVarSizeFields) {
        ptr = serialize_field(field, ptr);
    }
    return ptr;
}

Schema::Schema(const char* ptr) {
    // we can ignore the size
    ptr += sizeof(uint32_t);
    uint16_t numColumns = *reinterpret_cast<const uint16_t*>(ptr);
    ptr += sizeof(numColumns);
    uint8_t type = *reinterpret_cast<const uint8_t*>(ptr);
    mType = crossbow::from_underlying<TableType>(type);
    ptr += sizeof(type);
    uint8_t allNotNull = *reinterpret_cast<const uint8_t*>(ptr);
    mAllNotNull = allNotNull > 0;
    ptr += sizeof(allNotNull);
    for (uint16_t i = 0; i < numColumns; ++i) {
        FieldType type = *reinterpret_cast<const FieldType*>(ptr);
        ptr += sizeof(type);
        bool isNotNull = *reinterpret_cast<const bool*>(ptr);
        ptr += sizeof(isNotNull);
        uint16_t nameSize = *reinterpret_cast<const uint16_t*>(ptr);
        ptr += sizeof(nameSize);
        Field f(type, crossbow::string(ptr, nameSize), isNotNull);
        ptr += nameSize;
        if (f.isFixedSized()) {
            mFixedSizeFields.emplace_back(std::move(f));
        } else {
            mVarSizeFields.emplace_back(std::move(f));
        }
    }
}

Record::Record(Schema schema)
        : mSchema(std::move(schema)),
          mFieldMetaData(mSchema.fixedSizeFields().size() + mSchema.varSizeFields().size()) {
    int32_t currOffset = mSchema.allNotNull() ? 0 : (mFieldMetaData.size() + 7)/8;
    currOffset += (currOffset % 8) ? 8 - (currOffset % 8) : 0;

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

size_t Record::sizeOfTuple(const GenericTuple& tuple) const
{
    size_t result = mSchema.allNotNull() ? 0 : (mFieldMetaData.size() + 7)/8;
    result = ((result + 7u) & -8u);
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
    return ((result + 7u) & -8u);
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
        pos = mFieldMetaData[baseId].second;
        LOG_ASSERT(pos >= 0, "Offset for first variable length field is smaller than 0");

        for (; baseId < mFieldMetaData.size(); ++baseId) {
            // we know, that now all fields are variable length - that means the first four bytes are always the field
            // size
            pos += *reinterpret_cast<const uint32_t* const>(ptr + pos) + sizeof(uint32_t);
        }
    }
    // we have to make sure that the size of a tuple is 8 byte aligned
    return ((pos + 7u) & -8u);
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
    using uchar = unsigned char;
    char* res = result;
    auto headerSize = mSchema.allNotNull() ? 0 : (mFieldMetaData.size() + 7)/8;
    headerSize = ((headerSize + 7u) & -8u);
    char* current = res + headerSize;
    memset(res, 0, recSize);
    for (id_t id = 0; id < mFieldMetaData.size(); ++id) {
        auto& f = mFieldMetaData[id];
        const auto& name = f.first.name();
        LOG_ASSERT(f.second == std::numeric_limits<int32_t>::min() || current == (res + f.second),
                "Trying to write fixed size field to wrong offset");

        // first we need to check whether the value for this field is given
        auto iter = tuple.find(name);
        if (iter == tuple.end()) {
            // No value is given so set the field to NULL if the field can be NULL (abort when it must not be NULL)
            if (f.first.isNotNull()) {
                return false;
            }

            // In this case we set the field to NULL
            uchar& bitmap = *reinterpret_cast<uchar*>(res + id / 8);
            uchar pos = uchar(id % 8);
            bitmap |= uchar(0x1 << pos);

            // We store a default value for fields, even if the field is null. This is for performance reason (it makes
            // it faster to access any fixed-size fields). As the memset already set everything to 0 we only increase
            // the pointer.
            current += f.first.defaultSize();
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
    if (isNull) {
        return nullptr;
    }
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
    uchar bitmap = *reinterpret_cast<const uchar* const>(ptr + id / 8);
    unsigned char pos = uchar(id % 8);
    return (uchar(0x1 << pos) & bitmap) == 0;
}

} // namespace store
} // namespace tell
