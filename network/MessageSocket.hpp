#pragma once

#include <network/ErrorCode.hpp>
#include <network/MessageTypes.hpp>

#include <crossbow/infinio/InfinibandBuffer.hpp>
#include <crossbow/infinio/InfinibandSocket.hpp>

#include <cstddef>
#include <cstdint>
#include <system_error>

namespace tell {
namespace store {

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
        mPos += ((reinterpret_cast<const uintptr_t>(mPos) % alignment != 0)
                 ? (alignment - (reinterpret_cast<const uintptr_t>(mPos) % alignment))
                 :  0);
    }

    BufferReader extract(size_t length) {
        auto value = BufferReader(mPos, length);
        mPos += length;
        return value;
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
        mPos += ((reinterpret_cast<uintptr_t>(mPos) % alignment != 0)
                 ? (alignment - (reinterpret_cast<uintptr_t>(mPos) % alignment))
                 :  0);
    }

    BufferWriter extract(size_t length) {
        auto value = BufferWriter(mPos, length);
        mPos += length;
        return value;
    }

private:
    char* mPos;
    char* mEnd;
};

template <typename Handler>
class MessageSocket : protected crossbow::infinio::InfinibandSocketHandler {
protected:
    MessageSocket(crossbow::infinio::InfinibandSocket socket)
            : mSocket(std::move(socket)),
              mBuffer(crossbow::infinio::InfinibandBuffer::INVALID_ID),
              mSendBuffer(static_cast<char*>(nullptr), 0),
              mFlush(false) {
    }

    void init() {
        mSocket->setHandler(this);
    }

    /**
     * @brief Allocates buffer space for a message
     *
     * The implementation performs message batching: Messages share a common send buffer which is scheduled for sending
     * each poll interval.
     *
     * @param transactionId The transaction ID associated with the message to be written
     * @param messageType The type of the message to be written
     * @param messageLength The length of the message to be written
     * @param ec Error in case allocating the buffer failed
     * @return The buffer to write the message content
     */
    BufferWriter writeMessage(uint64_t transactionId, uint32_t messageType, uint32_t messageLength,
            std::error_code& ec);


    crossbow::infinio::InfinibandSocket mSocket;

private:
    static constexpr size_t HEADER_SIZE = sizeof(uint64_t) + 2 * sizeof(uint32_t);

    /**
     * @brief Callback function invoked by the Infinibadn socket.
     *
     * Reads all messages contained in the receive buffer and for every message invokes the onMessage callback on the
     * handler.
     */
    virtual void onReceive(const void* buffer, size_t length, const std::error_code& ec) final override;

    /**
     * @brief Callback function invoked by the Infiniband socket.
     *
     * Checks if the send was successfull and if it was not invokes the onSocketError callback on the handler.
     */
    virtual void onSend(uint32_t userId, const std::error_code& ec) final override;

    /**
     * @brief Sends the current send buffer
     *
     * @param ec Error in case the send failed
     */
    void sendCurrentBuffer(std::error_code& ec);

    /**
     * @brief Schedules the socket's send buffer to be flushed
     *
     * If the flush is not yet pending this enqueues a task on the socket's completion context to send the current send
     * buffer.
     *
     * @param ec Error in case the scheduling failed
     */
    void scheduleFlush(std::error_code& ec);

    /// The current Infiniband send buffer
    crossbow::infinio::InfinibandBuffer mBuffer;

    /// A buffer writer to write data to the current Infiniband buffer
    BufferWriter mSendBuffer;

    /// Whether a flush task is pending
    bool mFlush;
};

template <typename Handler>
BufferWriter MessageSocket<Handler>::writeMessage(uint64_t transactionId, uint32_t messageType, uint32_t messageLength,
        std::error_code& ec) {
    auto length = HEADER_SIZE + messageLength;
    if (!mSendBuffer.canWrite(length)) {
        sendCurrentBuffer(ec);
        if (ec) {
            return BufferWriter(nullptr, 0x0u);
        }

        mBuffer = mSocket->acquireSendBuffer();
        if (!mBuffer.valid()) {
            ec = error::invalid_buffer;
            return BufferWriter(nullptr, 0x0u);
        }
        mSendBuffer = BufferWriter(reinterpret_cast<char*>(mBuffer.data()), mBuffer.length());

        scheduleFlush(ec);
        if (ec) {
            return BufferWriter(nullptr, 0x0u);
        }
    }

    mSendBuffer.write<uint64_t>(transactionId);
    mSendBuffer.write<uint32_t>(messageType);
    mSendBuffer.write<uint32_t>(messageLength);

    auto message = mSendBuffer.extract(messageLength);

    mSendBuffer.align(sizeof(uint64_t));

    return message;
}

template <typename Handler>
void MessageSocket<Handler>::onReceive(const void* buffer, size_t length, const std::error_code& ec) {
    if (ec) {
        static_cast<Handler*>(this)->onSocketError(ec);
        return;
    }

    BufferReader receiveBuffer(reinterpret_cast<const char*>(buffer), length);
    while (receiveBuffer.canRead(HEADER_SIZE)) {
        auto transactionId = receiveBuffer.read<uint64_t>();
        auto messageType = receiveBuffer.read<uint32_t>();
        auto messageLength = receiveBuffer.read<uint32_t>();

        if (!receiveBuffer.canRead(messageLength)) {
            static_cast<Handler*>(this)->onSocketError(error::invalid_message);
            return;
        }
        auto message = receiveBuffer.extract(messageLength);

        static_cast<Handler*>(this)->onMessage(transactionId, messageType, message);

        receiveBuffer.align(sizeof(uint64_t));
    }
}

template <typename Handler>
void MessageSocket<Handler>::onSend(uint32_t userId, const std::error_code& ec) {
    if (ec) {
        static_cast<Handler*>(this)->onSocketError(ec);
        return;
    }
}

template <typename Handler>
void MessageSocket<Handler>::sendCurrentBuffer(std::error_code& ec) {
    if (!mBuffer.valid()) {
        return;
    }

    auto bytesWritten = static_cast<size_t>(mSendBuffer.data() - reinterpret_cast<char*>(mBuffer.data()));
    mBuffer.shrink(bytesWritten);

    mSocket->send(mBuffer, 0, ec);
    if (ec)  {
        mSocket->releaseSendBuffer(mBuffer);
    }
    mBuffer = crossbow::infinio::InfinibandBuffer(crossbow::infinio::InfinibandBuffer::INVALID_ID);
    mSendBuffer = BufferWriter(static_cast<char*>(nullptr), 0);
}

template <typename Handler>
void MessageSocket<Handler>::scheduleFlush(std::error_code& ec) {
    if (mFlush) {
        return;
    }

    mSocket->execute([this] () {
        // Check if buffer is valid
        if (!mBuffer.valid()) {
            return;
        }

        // Check if buffer has any data written
        if (mSendBuffer.data() == reinterpret_cast<char*>(mBuffer.data())) {
            mSocket->releaseSendBuffer(mBuffer);
            mBuffer = crossbow::infinio::InfinibandBuffer(crossbow::infinio::InfinibandBuffer::INVALID_ID);
            mSendBuffer = BufferWriter(static_cast<char*>(nullptr), 0);
            return;
        }

        std::error_code ec;
        sendCurrentBuffer(ec);
        if (ec) {
            static_cast<Handler*>(this)->onSocketError(ec);
            return;
        }

        mFlush = false;
    }, ec);
    if (ec) {
        return;
    }

    mFlush = true;
}

} // namespace store
} // namespace tell
