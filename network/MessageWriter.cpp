#include "MessageWriter.hpp"

namespace tell {
namespace store {

void MessageWriter::writeErrorResponse(uint64_t transactionId, error::server_errors error, std::error_code& ec) {
    auto message = writeResponse(transactionId, ResponseType::ERROR, sizeof(uint64_t), ec);
    if (ec) {
        return;
    }

    message.write<uint64_t>(static_cast<uint64_t>(error));
}

void MessageWriter::flush(std::error_code& ec) {
    if (!mBuffer.valid()) {
        return;
    }

    if (mPos != reinterpret_cast<char*>(mBuffer.data())) {
        sendCurrentBuffer(ec);
    } else {
        mSocket.releaseSendBuffer(mBuffer);
    }
}

BufferWriter MessageWriter::writeMessage(uint64_t transactionId, uint64_t type, size_t length, std::error_code& ec) {
    alignPointer(mPos, sizeof(uint64_t));

    auto messageSize = 2 * sizeof(uint64_t) + length;
    if (mPos + messageSize > mEnd) {
        sendCurrentBuffer(ec);
        if (ec) {
            return BufferWriter(nullptr, 0x0u);
        }

        mBuffer = mSocket.acquireSendBuffer();
        if (!mBuffer.valid()) {
            ec = error::invalid_buffer;
            return BufferWriter(nullptr, 0x0u);
        }
        mPos = reinterpret_cast<char*>(mBuffer.data());
        mEnd = mPos + mBuffer.length();
    }

    auto message = BufferWriter(mPos, messageSize);
    mPos += messageSize;

    message.write<uint64_t>(transactionId);
    message.write<uint64_t>(type);
    return message;
}

void MessageWriter::sendCurrentBuffer(std::error_code& ec) {
    if (!mBuffer.valid()) {
        return;
    }

    alignPointer(mPos, sizeof(uint64_t));
    auto bytesWritten = static_cast<size_t>(mPos - reinterpret_cast<char*>(mBuffer.data()));
    mBuffer.shrink(bytesWritten);

    mSocket.send(mBuffer, 0, ec);
    if (ec)  {
        mSocket.releaseSendBuffer(mBuffer);
    }
}

} // namespace store
} // namespace tell
