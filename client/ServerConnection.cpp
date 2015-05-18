#include "ServerConnection.hpp"

#include "TransactionManager.hpp"

#include <util/Epoch.hpp>
#include <util/ErrorCode.hpp>
#include <util/Logging.hpp>
#include <util/MessageTypes.hpp>
#include <util/MessageWriter.hpp>
#include <util/Record.hpp>
#include <util/SnapshotDescriptor.hpp>

#include <crossbow/infinio/Endpoint.hpp>
#include <crossbow/infinio/InfinibandBuffer.hpp>

namespace tell {
namespace store {

namespace {

void writeSnapshot(BufferWriter& message, const SnapshotDescriptor& snapshot) {
    message.write<uint64_t>(snapshot.version());
    // TODO Implement snapshot caching
    message.write<uint8_t>(0x0u); // Cached
    message.write<uint8_t>(0x1u); // HasDescriptor
    message.align(sizeof(uint32_t));
    auto descriptorLength = snapshot.length();
    message.write<uint32_t>(descriptorLength);
    message.write(snapshot.descriptor(), descriptorLength);
}

} // anonymous namespace

ServerConnection::~ServerConnection() {
    boost::system::error_code ec;
    mSocket.close(ec);
    if (ec) {
        // TODO Handle this situation somehow (this should probably not happen at this point)
    }
}

void ServerConnection::connect(const crossbow::string& host, uint16_t port, boost::system::error_code& ec) {
    LOG_INFO("Connecting to TellStore server %1%:%2%", host, port);

    // Open socket
    mSocket.open(ec);
    if (ec) {
        return;
    }
    mSocket.setHandler(this);

    // Connect to remote server
    mSocket.connect(crossbow::infinio::Endpoint(crossbow::infinio::Endpoint::ipv4(), host, port), ec);
}

void ServerConnection::shutdown() {
    boost::system::error_code ec;
    mSocket.disconnect(ec);
    if (ec) {
        LOG_ERROR("Error disconnecting");
        // TODO Handle this situation somehow - Can this even happen?
    }
}

void ServerConnection::createTable(uint64_t transactionId, const crossbow::string& name, const Schema& schema,
        boost::system::error_code& ec) {
    auto nameSize = name.size();
    auto schemaSize = schema.schemaSize();
    auto messageSize = sizeof(uint16_t) + nameSize;
    messageSize += ((messageSize % sizeof(uint64_t) != 0)
            ? (sizeof(uint64_t) - (messageSize % sizeof(uint64_t)))
            :  0);
    messageSize += schemaSize;

    MessageWriter writer(mSocket);
    auto request = writer.writeRequest(transactionId, RequestType::CREATE_TABLE, messageSize, ec);
    if (ec) {
        return;
    }
    request.write<uint16_t>(nameSize);
    request.write(name.data(), nameSize);

    request.align(sizeof(uint64_t));
    schema.serialize(request.data());
    request.advance(schemaSize);

    writer.flush(ec);
}

void ServerConnection::getTableId(uint64_t transactionId, const crossbow::string& name, boost::system::error_code& ec) {
    auto nameSize = name.size();
    auto messageSize = sizeof(uint16_t) + nameSize;

    MessageWriter writer(mSocket);
    auto request = writer.writeRequest(transactionId, RequestType::GET_TABLEID, messageSize, ec);
    if (ec) {
        return;
    }
    request.write<uint16_t>(nameSize);
    request.write(name.data(), nameSize);

    writer.flush(ec);
}

void ServerConnection::get(uint64_t transactionId, uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot,
        boost::system::error_code& ec) {
    auto messageSize = 4 * sizeof(uint64_t) + snapshot.length();

    MessageWriter writer(mSocket);
    auto message = writer.writeRequest(transactionId, RequestType::GET, messageSize, ec);
    if (ec) {
        return;
    }
    message.write<uint64_t>(tableId);
    message.write<uint64_t>(key);
    writeSnapshot(message, snapshot);

    writer.flush(ec);
}

void ServerConnection::getNewest(uint64_t transactionId, uint64_t tableId, uint64_t key,
        boost::system::error_code& ec) {
    auto messageSize = 2 * sizeof(uint64_t);

    MessageWriter writer(mSocket);
    auto message = writer.writeRequest(transactionId, RequestType::GET_NEWEST, messageSize, ec);
    if (ec) {
        return;
    }
    message.write<uint64_t>(tableId);
    message.write<uint64_t>(key);

    writer.flush(ec);
}

void ServerConnection::update(uint64_t transactionId, uint64_t tableId, uint64_t key, size_t size, const char* data,
        const SnapshotDescriptor& snapshot, boost::system::error_code& ec) {
    auto messageSize = 3 * sizeof(uint64_t) + size;
    messageSize += ((messageSize % sizeof(uint64_t) != 0)
            ? (sizeof(uint64_t) - (messageSize % sizeof(uint64_t)))
            :  0);
    messageSize += 2 * sizeof(uint64_t) + snapshot.length();

    MessageWriter writer(mSocket);
    auto message = writer.writeRequest(transactionId, RequestType::UPDATE, messageSize, ec);
    if (ec) {
        return;
    }
    message.write<uint64_t>(tableId);
    message.write<uint64_t>(key);

    message.align(sizeof(uint32_t));
    message.write<uint32_t>(size);
    message.write(data, size);

    message.align(sizeof(uint64_t));
    writeSnapshot(message, snapshot);

    writer.flush(ec);
}

void ServerConnection::insert(uint64_t transactionId, uint64_t tableId, uint64_t key, size_t size, const char* data,
        const SnapshotDescriptor& snapshot, bool succeeded, boost::system::error_code& ec) {
    auto messageSize = 3 * sizeof(uint64_t) + size;
    messageSize += ((messageSize % sizeof(uint64_t) != 0)
            ? (sizeof(uint64_t) - (messageSize % sizeof(uint64_t)))
            :  0);
    messageSize += 2 * sizeof(uint64_t) + snapshot.length();

    MessageWriter writer(mSocket);
    auto message = writer.writeRequest(transactionId, RequestType::INSERT, messageSize, ec);
    if (ec) {
        return;
    }
    message.write<uint64_t>(tableId);
    message.write<uint64_t>(key);
    message.write<uint8_t>(succeeded ? 0x1u : 0x0u);

    message.align(sizeof(uint32_t));
    message.write<uint32_t>(size);
    message.write(data, size);
    message.align(sizeof(uint64_t));

    writeSnapshot(message, snapshot);

    writer.flush(ec);
}

void ServerConnection::remove(uint64_t transactionId, uint64_t tableId, uint64_t key,
        const SnapshotDescriptor& snapshot, boost::system::error_code& ec) {
    auto messageSize = 4 * sizeof(uint64_t) + snapshot.length();

    MessageWriter writer(mSocket);
    auto message = writer.writeRequest(transactionId, RequestType::REMOVE, messageSize, ec);
    if (ec) {
        return;
    }
    message.write<uint64_t>(tableId);
    message.write<uint64_t>(key);

    writeSnapshot(message, snapshot);

    writer.flush(ec);
}

void ServerConnection::revert(uint64_t transactionId, uint64_t tableId, uint64_t key,
        const SnapshotDescriptor& snapshot, boost::system::error_code& ec) {
    auto messageSize = 4 * sizeof(uint64_t) + snapshot.length();

    MessageWriter writer(mSocket);
    auto message = writer.writeRequest(transactionId, RequestType::REVERT, messageSize, ec);
    if (ec) {
        return;
    }
    message.write<uint64_t>(tableId);
    message.write<uint64_t>(key);

    writeSnapshot(message, snapshot);

    writer.flush(ec);
}

void ServerConnection::onConnected(const boost::system::error_code& ec) {
    mManager.onConnected(ec);
}

void ServerConnection::onReceive(const void* buffer, size_t length, const boost::system::error_code& ec) {
    if (ec) {
        LOG_ERROR("Error receiving message");
        // TODO Handle this situation somehow
        return;
    }

    BufferReader responseReader(reinterpret_cast<const char*>(buffer), length);
    while (!responseReader.exhausted()) {
        auto transactionId = responseReader.read<uint64_t>();
        auto responseType = responseReader.read<uint64_t>();
        if (responseType > static_cast<uint64_t>(ResponseType::LAST)) {
            // Unknown request type
        }

        LOG_TRACE("T %1%] Handling response of type %2%", transactionId, responseType);
        mManager.handleResponse(transactionId, Response(static_cast<ResponseType>(responseType), &responseReader));

        responseReader.align(sizeof(uint64_t));
    }
}

void ServerConnection::onSend(uint32_t userId, const boost::system::error_code& ec) {
    if (ec) {
        LOG_ERROR("Error sending message [errcode = %1% %2%]", ec, ec.message());
        // TODO Handle this situation somehow
    }
}

void ServerConnection::onDisconnect() {
    shutdown();
}

void ServerConnection::onDisconnected() {
    // Abort all open contexts
    // No more handlers are active so we do not need to synchronize

    // TODO Impl
}

void ServerConnection::sendRequest(crossbow::infinio::InfinibandBuffer& buffer, boost::system::error_code& ec) {
    // TODO Implement actual request batching
    mSocket.send(buffer, 0x0u, ec);
    if (ec) {
        mSocket.releaseSendBuffer(buffer);
        return;
    }
}

void ServerConnection::Response::reset() {
    mMessage = nullptr;
    mType = ResponseType::ERROR;
}

bool ServerConnection::Response::createTable(uint64_t& tableId, boost::system::error_code& ec) {
    if (!checkMessage(ResponseType::CREATE_TABLE, ec)) {
        return false;
    }

    tableId = mMessage->read<uint64_t>();
    return (tableId != 0x0u);
}

bool ServerConnection::Response::getTableId(uint64_t& tableId, boost::system::error_code& ec) {
    if (!checkMessage(ResponseType::GET_TABLEID, ec)) {
        return false;
    }

    tableId = mMessage->read<uint64_t>();
    return (tableId != 0x0u);
}

bool ServerConnection::Response::get(size_t& size, const char*& data, uint64_t& version, bool& isNewest,
        boost::system::error_code& ec) {
    if (!checkMessage(ResponseType::GET, ec)) {
        return false;
    }

    version = mMessage->read<uint64_t>();
    isNewest = mMessage->read<uint8_t>();
    bool success = mMessage->read<uint8_t>();

    if (success) {
        mMessage->align(sizeof(uint32_t));
        size = mMessage->read<uint32_t>();

        auto dataBuffer = new char[size];
        memcpy(dataBuffer, mMessage->data(), size);
        mMessage->advance(size);
        data = dataBuffer;
    } else {
        size = 0x0u;
        data = nullptr;
    }

    return size != 0x0u;
}

bool ServerConnection::Response::modification(boost::system::error_code& ec) {
    if (!checkMessage(ResponseType::MODIFICATION, ec)) {
        return false;
    }

    bool succeeded = mMessage->read<uint8_t>();
    return succeeded;
}

bool ServerConnection::Response::checkMessage(ResponseType type, boost::system::error_code& ec) {
    LOG_ASSERT(mMessage, "Message is null");
    if (mType == ResponseType::ERROR) {
        ec = boost::system::error_code(*reinterpret_cast<uint64_t*>(mMessage), error::get_server_category());
        return false;
    }
    if (mType != type) {
        ec = error::unkown_response;
        return false;
    }

    return true;
}

} // namespace store
} // namespace tell
