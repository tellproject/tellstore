#include "ServerConnection.hpp"

#include "RpcMessages.pb.h"

#include "ErrorCode.hpp"
#include "ClientConfig.hpp"

#include <util/Epoch.hpp>
#include <util/Logging.hpp>
#include <util/Record.hpp>
#include <util/SnapshotDescriptor.hpp>

#include <crossbow/infinio/Endpoint.hpp>
#include <crossbow/infinio/InfinibandBuffer.hpp>

#include <mutex>

namespace tell {
namespace store {

namespace {

google::protobuf::Message* gDummyMessage;

} // anonymous namespace

ServerConnection::~ServerConnection() {
    boost::system::error_code ec;
    mSocket.close(ec);
    if (ec) {
        // TODO Handle this situation somehow (this should probably not happen at this point)
    }
}

void ServerConnection::init(const ClientConfig& config, boost::system::error_code& ec) {
    LOG_INFO("Connecting to TellStore server");

    // Open socket
    mSocket.open(ec);
    if (ec) {
        return;
    }
    mSocket.setHandler(this);

    // Connect to remote server
    ExecutionContextGuard context(*this, 0x0u, 0x0u, gDummyMessage, ec);
    mSocket.connect(crossbow::infinio::Endpoint(crossbow::infinio::Endpoint::ipv4(), config.server, config.port), ec);
    if (ec) {
        return;
    }
    context.waitForCompletion();
}

void ServerConnection::shutdown() {
    boost::system::error_code ec;
    mSocket.disconnect(ec);
    if (ec) {
        LOG_ERROR("Error disconnecting");
        // TODO Handle this situation somehow - Can this even happen?
    }
}

bool ServerConnection::createTable(const crossbow::string& name, const Schema& schema, uint64_t& tableId,
        boost::system::error_code& ec) {
    std::unique_ptr<proto::RpcRequest> request(new proto::RpcRequest());
    request->set_messageid(++mMessageId);

    auto createTableRequest = request->mutable_createtable();
    createTableRequest->set_name(name.c_str());

    char schemaData[schema.schemaSize()];
    schema.serialize(schemaData);
    createTableRequest->set_schema(schemaData);

    auto response = sendRequest<proto::CreateTableResponse>(std::move(request),
            proto::RpcResponse::kCreateTableFieldNumber, ec);
    if (ec) {
        return false;
    }

    if (response->has_tableid()) {
        tableId = response->tableid();
    }

    return response->has_tableid();
}

bool ServerConnection::get(uint64_t tableId, uint64_t key, size_t& size, const char*& data,
        const SnapshotDescriptor& snapshot, bool& isNewest, boost::system::error_code& ec) {
    std::unique_ptr<proto::RpcRequest> request(new proto::RpcRequest());
    request->set_messageid(++mMessageId);

    auto getRequest = request->mutable_get();
    getRequest->set_tableid(tableId);
    getRequest->set_key(key);

    // TODO Cache snapshot descriptor
    auto snapshotRequest = getRequest->mutable_snapshot();
    snapshotRequest->set_version(snapshot.version());
    snapshotRequest->set_data(snapshot.descriptor(), snapshot.length());
    snapshotRequest->set_cached(false);

    auto response = sendRequest<proto::GetResponse>(std::move(request), proto::RpcResponse::kGetFieldNumber, ec);
    if (ec) {
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

void ServerConnection::insert(uint64_t tableId, uint64_t key, size_t size, const char* data,
        const SnapshotDescriptor& snapshot, boost::system::error_code& ec, bool* succeeded) {
    std::unique_ptr<proto::RpcRequest> request(new proto::RpcRequest());
    request->set_messageid(++mMessageId);

    auto insertRequest = request->mutable_insert();
    insertRequest->set_tableid(tableId);
    insertRequest->set_key(key);
    insertRequest->set_data(data, size);

    // TODO Cache snapshot descriptor
    auto snapshotRequest = insertRequest->mutable_snapshot();
    snapshotRequest->set_version(snapshot.version());
    snapshotRequest->set_data(snapshot.descriptor(), snapshot.length());
    snapshotRequest->set_cached(false);

    if (succeeded) {
        insertRequest->set_succeeded(true);
    }

    auto response = sendRequest<proto::InsertResponse>(std::move(request), proto::RpcResponse::kInsertFieldNumber, ec);
    if (ec) {
        return;
    }

    if (succeeded) {
        *succeeded = response->succeeded();
    }
}

void ServerConnection::onConnected(const boost::system::error_code& ec) {
    tbb::queuing_rw_mutex::scoped_lock lock(mContextsMutex, false);
    auto i = mContexts.find(0x0u);
    LOG_ASSERT(i != mContexts.end(), "Context for connection event not found");
    auto& context = i->second;

    *(context.ec) = ec;

    context.cond->notify_one();
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
    tbb::queuing_rw_mutex::scoped_lock lock(mContextsMutex, false);
    auto i = mContexts.find(response.messageid());
    LOG_ASSERT(i != mContexts.end(), "Context for message ID %1% not found", response.messageid());
    auto& context = i->second;

    if (response.has_error()) {
        *(context.ec) = boost::system::error_code(response.error(), error::get_server_category());

        context.cond->notify_one();
        return;
    }

#define HANDLE_RESPONSE_CASE(_case, _name) \
    case _case: {\
        if (context.type != _case##FieldNumber) {\
            *(context.ec) = boost::system::error_code(proto::RpcResponse::UNKNOWN_RESPONSE,\
                    error::get_server_category());\
            break;\
        }\
        *(context.message) = response.release_ ## _name();\
    } break


    switch (response.Response_case()) {
    HANDLE_RESPONSE_CASE(proto::RpcResponse::kCreateTable, createtable);
    HANDLE_RESPONSE_CASE(proto::RpcResponse::kGet, get);
    HANDLE_RESPONSE_CASE(proto::RpcResponse::kInsert, insert);

    default: {
        *(context.ec) = boost::system::error_code(proto::RpcResponse::UNKNOWN_RESPONSE, error::get_server_category());
    } break;
    }

    context.cond->notify_one();
}

template<class Response>
std::unique_ptr<Response> ServerConnection::sendRequest(std::unique_ptr<proto::RpcRequest> request, int field,
        boost::system::error_code& ec) {
    auto id = request->messageid();

    // TODO Implement actual request batching
    proto::RpcRequestBatch requestBatch;
    auto requestField = requestBatch.mutable_request();
    requestField->AddAllocated(request.release());

    auto buffer = mSocket.acquireSendBuffer(requestBatch.ByteSize());
    if (buffer.id() == crossbow::infinio::InfinibandBuffer::INVALID_ID) {
        ec = error::invalid_buffer;
        return nullptr;
    }
    requestBatch.SerializeWithCachedSizesToArray(reinterpret_cast<uint8_t*>(buffer.data()));

    google::protobuf::Message* response = nullptr;
    ExecutionContextGuard context(*this, id, field, response, ec);
    // TODO This is a 64 to 32 bit conversion
    mSocket.send(buffer, id, ec);
    if (ec) {
        mSocket.releaseSendBuffer(buffer);
        return nullptr;
    }
    context.waitForCompletion();

    LOG_ASSERT(!ec && response, "Response is null despite indicating no error");

    return std::unique_ptr<Response>(static_cast<Response*>(response));
}

ServerConnection::ExecutionContextGuard::ExecutionContextGuard(ServerConnection& con, uint64_t id, int type,
        google::protobuf::Message*& message, boost::system::error_code& ec)
        : mConnection(con),
          mId(id) {
    tbb::queuing_rw_mutex::scoped_lock lock(mConnection.mContextsMutex, false);
    auto res = mConnection.mContexts.insert(
            std::make_pair(mId, ServerConnection::ExecutionContext {type, &message, &ec, &mCond})
    ).second;
    LOG_ASSERT(res, "Unable to insert context for message ID %1%", mId);
}

ServerConnection::ExecutionContextGuard::~ExecutionContextGuard() {
    tbb::queuing_rw_mutex::scoped_lock lock(mConnection.mContextsMutex, true);
    auto res = mConnection.mContexts.unsafe_erase(mId);
    LOG_ASSERT(res == 1, "Unable to remove context for message ID %1%", mId);
}

void ServerConnection::ExecutionContextGuard::waitForCompletion() {
    std::unique_lock<crossbow::mutex> lock(mMutex);
    mCond.wait(lock);
}

} // namespace store
} // namespace tell
