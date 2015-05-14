#include "ServerConnection.hpp"

#include "RpcMessages.pb.h"

#include "ErrorCode.hpp"
#include "TransactionManager.hpp"

#include <util/Epoch.hpp>
#include <util/Logging.hpp>
#include <util/Record.hpp>
#include <util/SnapshotDescriptor.hpp>

#include <crossbow/infinio/Endpoint.hpp>
#include <crossbow/infinio/InfinibandBuffer.hpp>

namespace tell {
namespace store {

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
    std::unique_ptr<proto::RpcRequest> request(new proto::RpcRequest());
    request->set_messageid(transactionId);

    auto createTableRequest = request->mutable_createtable();
    createTableRequest->set_name(name.c_str());

    auto schemaSize = schema.schemaSize();
    char schemaData[schemaSize];
    schema.serialize(schemaData);
    createTableRequest->set_schema(schemaData, schemaSize);

    sendRequest(std::move(request), ec);
}

void ServerConnection::get(uint64_t transactionId, uint64_t tableId, uint64_t key, const SnapshotDescriptor& snapshot,
        boost::system::error_code& ec) {
    std::unique_ptr<proto::RpcRequest> request(new proto::RpcRequest());
    request->set_messageid(transactionId);

    auto getRequest = request->mutable_get();
    getRequest->set_tableid(tableId);
    getRequest->set_key(key);

    // TODO Cache snapshot descriptor
    auto snapshotRequest = getRequest->mutable_snapshot();
    snapshotRequest->set_version(snapshot.version());
    snapshotRequest->set_data(snapshot.descriptor(), snapshot.length());
    snapshotRequest->set_cached(false);

    sendRequest(std::move(request), ec);
}

void ServerConnection::insert(uint64_t transactionId, uint64_t tableId, uint64_t key, size_t size, const char* data,
        const SnapshotDescriptor& snapshot, bool succeeded, boost::system::error_code& ec) {
    std::unique_ptr<proto::RpcRequest> request(new proto::RpcRequest());
    request->set_messageid(transactionId);

    auto insertRequest = request->mutable_insert();
    insertRequest->set_tableid(tableId);
    insertRequest->set_key(key);
    insertRequest->set_data(data, size);

    // TODO Cache snapshot descriptor
    auto snapshotRequest = insertRequest->mutable_snapshot();
    snapshotRequest->set_version(snapshot.version());
    snapshotRequest->set_data(snapshot.descriptor(), snapshot.length());
    snapshotRequest->set_cached(false);

    insertRequest->set_succeeded(succeeded);

    sendRequest(std::move(request), ec);
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

    proto::RpcResponseBatch responseBatch;
    if (!responseBatch.ParseFromArray(buffer, length)) {
        LOG_ERROR("Error parsing protobuf message");
        // TODO Handle this situation somehow
        return;
    }

    for (auto& response : *responseBatch.mutable_response()) {
        handleResponse(response);
    }
}

void ServerConnection::onSend(uint32_t userId, const boost::system::error_code& ec) {
    if (ec) {
        LOG_ERROR("Error sending message");
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

void ServerConnection::handleResponse(proto::RpcResponse& response) {
    if (response.has_error()) {
        mManager.handleResponse(response.messageid(), ServerConnection::Response(response.error()));
        return;
    }

#define HANDLE_RESPONSE_CASE(_case, _name) \
    case _case: {\
        mManager.handleResponse(response.messageid(),\
                ServerConnection::Response(_case##FieldNumber, response.release_ ## _name()));\
    } break

    switch (response.Response_case()) {
    HANDLE_RESPONSE_CASE(proto::RpcResponse::kCreateTable, createtable);
    HANDLE_RESPONSE_CASE(proto::RpcResponse::kGet, get);
    HANDLE_RESPONSE_CASE(proto::RpcResponse::kInsert, insert);
    default: {
        mManager.handleResponse(response.messageid(), ServerConnection::Response(proto::RpcResponse::UNKNOWN_RESPONSE));
    } break;
    }
}

void ServerConnection::sendRequest(std::unique_ptr<proto::RpcRequest> request, boost::system::error_code& ec) {
    // TODO Implement actual request batching
    proto::RpcRequestBatch requestBatch;
    auto requestField = requestBatch.mutable_request();
    requestField->AddAllocated(request.release());

    auto buffer = mSocket.acquireSendBuffer(requestBatch.ByteSize());
    if (buffer.id() == crossbow::infinio::InfinibandBuffer::INVALID_ID) {
        ec = error::invalid_buffer;
        return;
    }
    requestBatch.SerializeWithCachedSizesToArray(reinterpret_cast<uint8_t*>(buffer.data()));

    mSocket.send(buffer, 0x0u, ec);
    if (ec) {
        mSocket.releaseSendBuffer(buffer);
        return;
    }
}

void ServerConnection::Response::reset() {
    mMessage.reset();
    mType = 0x0u;
    mError = 0x0u;
}

bool ServerConnection::Response::createTable(uint64_t& tableId, boost::system::error_code& ec) {
    auto response = getMessage<proto::CreateTableResponse>(proto::RpcResponse::kCreateTableFieldNumber, ec);
    if (!response) {
        return false;
    }

    if (response->has_tableid()) {
        tableId = response->tableid();
    }

    return response->has_tableid();
}

bool ServerConnection::Response::get(size_t& size, const char*& data, bool& isNewest, boost::system::error_code& ec) {
    auto response = getMessage<proto::GetResponse>(proto::RpcResponse::kGetFieldNumber, ec);
    if (!response) {
        return false;
    }

    isNewest = response->isnewest();
    if (response->has_data()) {
        size = response->data().length();
        // TODO Prevent this copy
        auto dataBuffer = new char[size];
        memcpy(dataBuffer, response->data().data(), size);
        data = dataBuffer;
    }

    return response->has_data();
}

void ServerConnection::Response::insert(bool* succeeded, boost::system::error_code& ec) {
    auto response = getMessage<proto::InsertResponse>(proto::RpcResponse::kInsertFieldNumber, ec);
    if (!response) {
        return;
    }

    if (succeeded) {
        LOG_ASSERT(response->has_succeeded(), "Message has no succeeded");
        *succeeded = response->succeeded();
    }
}

template <typename R>
R* ServerConnection::Response::getMessage(int field, boost::system::error_code& ec) {
    if (mError) {
        ec = boost::system::error_code(mError, error::get_server_category());
        return nullptr;
    }
    if (mType != field) {
        ec = boost::system::error_code(proto::RpcResponse::UNKNOWN_RESPONSE, error::get_server_category());
        return nullptr;
    }
    LOG_ASSERT(!mError && mMessage.get(), "Message is null despite indicating no error");

    return static_cast<R*>(mMessage.get());
}

} // namespace store
} // namespace tell
