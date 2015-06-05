#include "ServerConnection.hpp"

#include "TransactionManager.hpp"

#include <network/ErrorCode.hpp>
#include <util/Epoch.hpp>
#include <util/helper.hpp>
#include <util/Logging.hpp>
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
    std::error_code ec;
    mSocket->close(ec);
    if (ec) {
        // TODO Handle this situation somehow (this should probably not happen at this point)
    }
}

void ServerConnection::connect(const crossbow::string& host, uint16_t port, uint64_t thread, std::error_code& ec) {
    LOG_INFO("Connecting to TellStore server %1%:%2%", host, port);

    MessageSocket::init();

    // Open socket
    mSocket->open(ec);
    if (ec) {
        return;
    }

    // Connect to remote server
    crossbow::infinio::Endpoint ep(crossbow::infinio::Endpoint::ipv4(), host, port);

    crossbow::string data;
    data.append(reinterpret_cast<char*>(&thread), sizeof(uint64_t));

    mSocket->connect(ep, data, ec);
}

void ServerConnection::shutdown() {
    std::error_code ec;
    mSocket->disconnect(ec);
    if (ec) {
        LOG_ERROR("Error disconnecting");
        // TODO Handle this situation somehow - Can this even happen?
    }
}

void ServerConnection::createTable(uint64_t transactionId, const crossbow::string& name, const Schema& schema,
        std::error_code& ec) {
    auto nameSize = name.size();
    auto schemaSize = schema.schemaSize();
    auto messageSize = sizeof(uint16_t) + nameSize;
    messageSize += ((messageSize % sizeof(uint64_t) != 0)
            ? (sizeof(uint64_t) - (messageSize % sizeof(uint64_t)))
            :  0);
    messageSize += schemaSize;

    auto request = writeRequest(transactionId, RequestType::CREATE_TABLE, messageSize, ec);
    if (ec) {
        return;
    }
    request.write<uint16_t>(nameSize);
    request.write(name.data(), nameSize);

    request.align(sizeof(uint64_t));
    schema.serialize(request.data());
    request.advance(schemaSize);
}

void ServerConnection::getTableId(uint64_t transactionId, const crossbow::string& name, std::error_code& ec) {
    auto nameSize = name.size();
    auto messageSize = sizeof(uint16_t) + nameSize;

    auto request = writeRequest(transactionId, RequestType::GET_TABLEID, messageSize, ec);
    if (ec) {
        return;
    }
    request.write<uint16_t>(nameSize);
    request.write(name.data(), nameSize);
}

void ServerConnection::get(uint64_t transactionId, uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot,
        std::error_code& ec) {
    auto messageSize = 4 * sizeof(uint64_t) + snapshot.length();

    auto request = writeRequest(transactionId, RequestType::GET, messageSize, ec);
    if (ec) {
        return;
    }
    request.write<uint64_t>(tableId);
    request.write<uint64_t>(key);
    writeSnapshot(request, snapshot);
}

void ServerConnection::getNewest(uint64_t transactionId, uint64_t tableId, uint64_t key, std::error_code& ec) {
    auto messageSize = 2 * sizeof(uint64_t);

    auto request = writeRequest(transactionId, RequestType::GET_NEWEST, messageSize, ec);
    if (ec) {
        return;
    }
    request.write<uint64_t>(tableId);
    request.write<uint64_t>(key);
}

void ServerConnection::update(uint64_t transactionId, uint64_t tableId, uint64_t key, const Record& record,
        const GenericTuple& tuple, const SnapshotDescriptor& snapshot, std::error_code& ec) {
    auto size = record.sizeOfTuple(tuple);
    doUpdate(transactionId, tableId, key, size, snapshot, [size, &record, &tuple] (BufferWriter& request) {
        if (!record.create(request.data(), tuple, size)) {
            return false;
        }
        request.advance(size);
        return true;
    }, ec);
}

void ServerConnection::update(uint64_t transactionId, uint64_t tableId, uint64_t key, size_t size, const char* data,
        const SnapshotDescriptor& snapshot, std::error_code& ec) {
    doUpdate(transactionId, tableId, key, size, snapshot, [size, data] (BufferWriter& request) {
        request.write(data, size);
        return true;
    }, ec);
}

void ServerConnection::insert(uint64_t transactionId, uint64_t tableId, uint64_t key, const Record& record,
        const GenericTuple& tuple, const SnapshotDescriptor& snapshot, bool succeeded, std::error_code& ec) {
    auto size = record.sizeOfTuple(tuple);
    doInsert(transactionId, tableId, key, size, snapshot, succeeded, [size, &record, &tuple] (BufferWriter& request) {
        if (!record.create(request.data(), tuple, size)) {
            return false;
        }
        request.advance(size);
        return true;
    }, ec);
}

void ServerConnection::insert(uint64_t transactionId, uint64_t tableId, uint64_t key, size_t size, const char* data,
        const SnapshotDescriptor& snapshot, bool succeeded, std::error_code& ec) {
    doInsert(transactionId, tableId, key, size, snapshot, succeeded, [size, data] (BufferWriter& request) {
        request.write(data, size);
        return true;
    }, ec);
}

void ServerConnection::remove(uint64_t transactionId, uint64_t tableId, uint64_t key,
        const SnapshotDescriptor& snapshot, std::error_code& ec) {
    auto messageSize = 4 * sizeof(uint64_t) + snapshot.length();

    auto request = writeRequest(transactionId, RequestType::REMOVE, messageSize, ec);
    if (ec) {
        return;
    }
    request.write<uint64_t>(tableId);
    request.write<uint64_t>(key);

    writeSnapshot(request, snapshot);
}

void ServerConnection::revert(uint64_t transactionId, uint64_t tableId, uint64_t key,
        const SnapshotDescriptor& snapshot, std::error_code& ec) {
    auto messageSize = 4 * sizeof(uint64_t) + snapshot.length();

    auto request = writeRequest(transactionId, RequestType::REVERT, messageSize, ec);
    if (ec) {
        return;
    }
    request.write<uint64_t>(tableId);
    request.write<uint64_t>(key);

    writeSnapshot(request, snapshot);
}

void ServerConnection::scan(uint64_t transactionId, uint64_t tableId, uint16_t id,
        const crossbow::infinio::LocalMemoryRegion& destRegion, uint32_t querySize, const char* query,
        const SnapshotDescriptor& snapshot, std::error_code& ec) {
    auto messageSize = 5 * sizeof(uint64_t) + querySize;
    messageSize += ((messageSize % sizeof(uint64_t) != 0)
            ? (sizeof(uint64_t) - (messageSize % sizeof(uint64_t)))
            :  0);
    messageSize += 2 * sizeof(uint64_t) + snapshot.length();

    auto request = writeRequest(transactionId, RequestType::SCAN, messageSize, ec);
    if (ec) {
        return;
    }
    request.write<uint64_t>(tableId);
    request.write<uint16_t>(id);

    request.align(sizeof(uint64_t));
    request.write<uint64_t>(destRegion.address());
    request.write<uint64_t>(destRegion.length());
    request.write<uint32_t>(destRegion.rkey());

    request.write<uint32_t>(querySize);
    request.write(query, querySize);

    request.align(sizeof(uint64_t));
    writeSnapshot(request, snapshot);
}

void ServerConnection::onConnected(const crossbow::string& data, const std::error_code& ec) {
    mProcessor.onConnected(ec);
}

void ServerConnection::onMessage(uint64_t transactionId, uint32_t messageType, BufferReader message) {
    if (messageType > to_underlying(ResponseType::LAST)) {
        messageType = to_underlying(ResponseType::UNKOWN);
    }

    LOG_TRACE("T %1%] Handling response of type %2%", transactionId, messageType);
    mProcessor.handleResponse(transactionId, Response(from_underlying<ResponseType>(messageType), &message));
}

void ServerConnection::onSocketError(const std::error_code& ec) {
    LOG_ERROR("Error during socket operation [error = %1% %2%]", ec, ec.message());
    shutdown();
}

void ServerConnection::onImmediate(uint32_t data) {
    mProcessor.handleScanProgress(static_cast<uint16_t>(data >> 16), static_cast<uint16_t>(data & 0xFFFFu));
}

void ServerConnection::onDisconnect() {
    shutdown();
}

void ServerConnection::onDisconnected() {
    // Abort all open contexts
    // No more handlers are active so we do not need to synchronize

    // TODO Impl
}

void ServerConnection::Response::reset() {
    mMessage = nullptr;
    mType = ResponseType::UNKOWN;
}

bool ServerConnection::Response::createTable(uint64_t& tableId, std::error_code& ec) {
    if (!checkMessage(ResponseType::CREATE_TABLE, ec)) {
        return false;
    }

    tableId = mMessage->read<uint64_t>();
    return (tableId != 0x0u);
}

bool ServerConnection::Response::getTableId(uint64_t& tableId, std::error_code& ec) {
    if (!checkMessage(ResponseType::GET_TABLEID, ec)) {
        return false;
    }

    tableId = mMessage->read<uint64_t>();
    return (tableId != 0x0u);
}

bool ServerConnection::Response::get(size_t& size, const char*& data, uint64_t& version, bool& isNewest,
        std::error_code& ec) {
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

bool ServerConnection::Response::modification(std::error_code& ec) {
    if (!checkMessage(ResponseType::MODIFICATION, ec)) {
        return false;
    }

    bool succeeded = mMessage->read<uint8_t>();
    return succeeded;
}

void ServerConnection::Response::scan(uint16_t& scanId, std::error_code& ec) {
    if (!checkMessage(ResponseType::SCAN, ec)) {
        return;
    }

    scanId = mMessage->read<uint16_t>();
    return;
}

bool ServerConnection::Response::checkMessage(ResponseType type, std::error_code& ec) {
    LOG_ASSERT(mMessage, "Message is null");
    if (mType == ResponseType::ERROR) {
        auto error = mMessage->read<uint64_t>();
        ec = std::error_code(error, error::get_server_category());
        return false;
    }
    if (mType != type) {
        ec = error::unkown_response;
        return false;
    }

    return true;
}

template <typename Fun>
void ServerConnection::doUpdate(uint64_t transactionId, uint64_t tableId, uint64_t key, size_t size,
        const SnapshotDescriptor& snapshot, Fun f, std::error_code& ec) {
    auto messageSize = 3 * sizeof(uint64_t) + size;
    messageSize += ((messageSize % sizeof(uint64_t) != 0)
            ? (sizeof(uint64_t) - (messageSize % sizeof(uint64_t)))
            :  0);
    messageSize += 2 * sizeof(uint64_t) + snapshot.length();

    auto request = writeRequest(transactionId, RequestType::UPDATE, messageSize, ec);
    if (ec) {
        return;
    }
    request.write<uint64_t>(tableId);
    request.write<uint64_t>(key);

    request.align(sizeof(uint32_t));
    request.write<uint32_t>(size);
    if (!f(request)) {
        revertMessage();
        ec = error::invalid_tuple;
        return;
    }

    request.align(sizeof(uint64_t));
    writeSnapshot(request, snapshot);
}

template <typename Fun>
void ServerConnection::doInsert(uint64_t transactionId, uint64_t tableId, uint64_t key, size_t size,
        const SnapshotDescriptor& snapshot, bool succeeded, Fun f, std::error_code& ec) {
    auto messageSize = 3 * sizeof(uint64_t) + size;
    messageSize += ((messageSize % sizeof(uint64_t) != 0)
            ? (sizeof(uint64_t) - (messageSize % sizeof(uint64_t)))
            :  0);
    messageSize += 2 * sizeof(uint64_t) + snapshot.length();

    auto request = writeRequest(transactionId, RequestType::INSERT, messageSize, ec);
    if (ec) {
        return;
    }
    request.write<uint64_t>(tableId);
    request.write<uint64_t>(key);
    request.write<uint8_t>(succeeded ? 0x1u : 0x0u);

    request.align(sizeof(uint32_t));
    request.write<uint32_t>(size);
    if (!f(request)) {
        revertMessage();
        ec = error::invalid_tuple;
        return;
    }

    request.align(sizeof(uint64_t));
    writeSnapshot(request, snapshot);
}

} // namespace store
} // namespace tell
