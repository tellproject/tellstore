#pragma once

#include <network/ErrorCode.hpp>
#include <network/MessageTypes.hpp>

#include <crossbow/infinio/InfinibandBuffer.hpp>
#include <crossbow/infinio/InfinibandSocket.hpp>

#include <boost/system/error_code.hpp>

#include <cstddef>
#include <cstdint>

namespace tell {
namespace store {

namespace {

void alignPointer(const char*& ptr, size_t alignment) {
    ptr += ((reinterpret_cast<const uintptr_t>(ptr) % alignment != 0)
            ? (alignment - (reinterpret_cast<const uintptr_t>(ptr) % alignment))
            :  0);
}

void alignPointer(char*& ptr, size_t alignment) {
    ptr += ((reinterpret_cast<uintptr_t>(ptr) % alignment != 0)
            ? (alignment - (reinterpret_cast<uintptr_t>(ptr) % alignment))
            :  0);
}

} // anonymous namespace

/**
 * @brief The BufferReader class used to read values from a buffer
 */
class BufferReader {
public:
    BufferReader(const char* pos, size_t length)
            : mPos(pos),
              mEnd(pos + length) {
    }

    bool exhausted() const {
        return (mPos >= mEnd);
    }

    bool canRead(size_t length) {
        return (mPos + length <= mEnd);
    }

    template <typename T>
    T read() {
        auto value = (*reinterpret_cast<const T*>(mPos));
        mPos += sizeof(T);
        return value;
    }

    const char* read(size_t length) {
        auto value = mPos;
        mPos += length;
        return value;
    }

    const char* data() const {
        return mPos;
    }

    void advance(size_t length) {
        mPos += length;
    }

    void align(size_t alignment) {
        alignPointer(mPos, alignment);
    }

private:
    const char* mPos;
    const char* mEnd;
};

/**
 * @brief The BufferWriter class used to write values to a buffer
 */
class BufferWriter {
public:
    BufferWriter(char* pos, size_t length)
            : mPos(pos),
              mEnd(pos + length) {
    }

    bool exhausted() const {
        return (mPos >= mEnd);
    }

    bool canWrite(size_t length) const {
        return (mPos + length <= mEnd);
    }

    template <typename T>
    void write(T value) {
        (*reinterpret_cast<T*>(mPos)) = value;
        mPos += sizeof(T);
    }

    void write(const void* value, size_t length) {
        memcpy(mPos, value, length);
        mPos += length;
    }

    char* data() {
        return mPos;
    }

    void advance(size_t length) {
        mPos += length;
    }

    void align(size_t alignment) {
        alignPointer(mPos, alignment);
    }

private:
    char* mPos;
    char* mEnd;
};

class MessageWriter {
public:
    MessageWriter(crossbow::infinio::InfinibandSocket& socket)
            : mSocket(socket),
              mBuffer(crossbow::infinio::InfinibandBuffer::INVALID_ID),
              mPos(static_cast<char*>(nullptr)),
              mEnd(mPos) {
    }

    BufferWriter writeRequest(uint64_t transactionId, RequestType request, size_t length,
            boost::system::error_code& ec) {
        return writeMessage(transactionId, static_cast<uint64_t>(request), length, ec);
    }

    BufferWriter writeResponse(uint64_t transactionId, ResponseType response, size_t length,
            boost::system::error_code& ec) {
        return writeMessage(transactionId, static_cast<uint64_t>(response), length, ec);
    }

    /**
     * @brief Writes the error response back to the client
     *
     * @param transactionId The target transaction ID
     * @param error Error code to send
     * @param ec Error in case the send failed
     */
    void writeErrorResponse(uint64_t transactionId, error::server_errors error, boost::system::error_code& ec);

    /**
     * @brief Flush any unsent buffer
     *
     * @param ec Error in case the flush failed
     */
    void flush(boost::system::error_code& ec);

private:
    BufferWriter writeMessage(uint64_t transactionId, uint64_t type, size_t length, boost::system::error_code& ec);

    void sendCurrentBuffer(boost::system::error_code& ec);

    crossbow::infinio::InfinibandSocket& mSocket;
    crossbow::infinio::InfinibandBuffer mBuffer;
    char* mPos;
    char* mEnd;
};

} // namespace store
} // namespace tell
