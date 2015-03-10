#include "Record.hpp"
#include "Logging.hpp"

namespace tell {
namespace store {


Field::Field(Field&& other)
    : mType(other.mType)
    , mName(std::move(other.mName))
    , mNotNull(other.mNotNull)
    , mData(other.mData)
{}

Field::Field(const Field& other)
    : mType(other.mType)
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


size_t Field::staticSize() const {
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
    uint8_t allNotNull = uint8_t(mAllNotNull ? 1 : 0);
    memcpy(ptr, &allNotNull, sizeof(allNotNull));
    ptr += 2;
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
    uint8_t allNotNull = *reinterpret_cast<const uint8_t*>(ptr);
    mAllNotNull = allNotNull > 0;
    ptr += 2;
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

Record::Record(const Schema& schema)
    : mSchema(schema), mFieldMetaData(schema.fixedSizeFields().size() + schema.varSizeFields().size()) {
    off_t headOffset = 4;
    {
        if (schema.varSizeFields().size() > 0) {
            headOffset += (mFieldMetaData.size() + 7) / 8;
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

const char* Record::data(const char* const ptr, Record::id_t id, bool& isNull, FieldType* type /* = nullptr*/) const {
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
        uchar bitmap = *reinterpret_cast<const uchar* const>(ptr + 4 + id / 8);
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
            // we know, that now all fields are variable mLength - that means the first two bytes are always the
            // field size
            pos += *reinterpret_cast<const uint16_t* const>(ptr + pos) + sizeof(uint16_t);
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

const char* MultiVersionRecord::getRecord(const SnapshotDescriptor& desc, const char* record, bool& isNewest) {
    isNewest = true;
    uint32_t numVersions = *reinterpret_cast<const uint32_t*>(record + 4);
    const uint64_t* versions = reinterpret_cast<const uint64_t*>(record + 8);
    const uint32_t* offsets = reinterpret_cast<const uint32_t*>(record + 8 + 8 * numVersions);
    for (uint32_t i = numVersions; i > 0; --i) {
        if (desc.inReadSet(versions[i])) {
            return (record + offsets[i]);
        } else {
            isNewest = false;
        }
    }
    return nullptr;
}

uint64_t MultiVersionRecord::getSmallestVersion(const char* record) {
    return *reinterpret_cast<const uint64_t*>(record + 8);
}

uint64_t MultiVersionRecord::getBiggestVersion(const char* record) {
    auto numVersions = *reinterpret_cast<const uint32_t*>(record + 4);
    return *(reinterpret_cast<const uint64_t*>(record + 8) + numVersions - 1);
}

uint32_t MultiVersionRecord::compact(char* record, uint64_t minVersion) {
    uint32_t& recSize = *reinterpret_cast<uint32_t*>(record);
    uint32_t result = recSize;
    uint32_t& numVersions = *reinterpret_cast<uint32_t*>(record + 4);
    uint64_t* versions = reinterpret_cast<uint64_t*>(record + 8);
    uint32_t* offsets = reinterpret_cast<uint32_t*>(record + 8 + 8*numVersions);
    char* data = record + 8 + 8*numVersions + 4*numVersions + (numVersions % 2 == 0 ? 0 : 4);
    uint32_t cleanTo = 0ul;
    for (; cleanTo < numVersions; ++cleanTo) {
        if (versions[cleanTo] >= minVersion) {
            break;
        }
    }
    if (cleanTo == numVersions && offsets[cleanTo - 1] == 0) {
        // In this case, we can completely remove the record
        return 0u;
    }
    if (cleanTo == 0ul) {
        // In this case, we can not clean anything
        return result;
    }
    // If the newest version is in smaller than minVersion, we still need to
    // keep the newest version
    cleanTo = cleanTo == numVersions ? numVersions - 1 : cleanTo;
    // we compact the versions array
    memmove(versions, versions + cleanTo*sizeof(uint64_t), (numVersions - cleanTo)*sizeof(uint64_t));
    recSize -= cleanTo*sizeof(uint64_t);
    // we need to clean the data up to offsets[cleanTo]
    auto cleanOffset = offsets[cleanTo];
    // now we can also compact the offset array
    int32_t padding = 0;
    // calculate the new padding
    if (numVersions % 2 == 0 && numVersions - cleanTo % 2 != 0) {
        padding = 4;
    } else if (numVersions % 2 != 0 && numVersions - cleanTo % 2 == 0) {
        padding = -4;
    }
    memmove(offsets, offsets + cleanTo*sizeof(uint32_t), (numVersions - cleanTo)*sizeof(uint32_t));
    for (uint32_t i = 0u; i < (numVersions - cleanTo); ++i) {
        offsets[i] -= cleanOffset + padding;
    }
    recSize -= cleanTo*sizeof(uint32_t);
    // finally, we can also copy back the data
    numVersions = numVersions - cleanTo;
    auto lastRecOff = offsets[numVersions - 1];
    auto sizeOfLastRec = *reinterpret_cast<uint32_t*>(record + lastRecOff);
    auto newDataOffset = 8 + 8*numVersions + 4*numVersions + (numVersions % 2 == 0 ? 0 : 4);
    // the size of all records is the offset of the last record plus the size of the last record minus the offset
    // from the beginning of the record
    memmove(record + newDataOffset, data, lastRecOff + sizeOfLastRec - newDataOffset);
    recSize = lastRecOff + sizeOfLastRec;
    // We write down the new number of versions
    assert(result > recSize);
    return result;
}
} // namespace store
} // namespace tell
