#include "Table.hpp"

#include <crossbow/infinio/ByteBuffer.hpp>
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

std::unique_ptr<Tuple> Tuple::deserialize(crossbow::infinio::BufferReader& reader) {
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

} // namespace store
} // namespace tell
