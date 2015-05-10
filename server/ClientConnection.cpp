#include "ClientConnection.hpp"

#include "ConnectionManager.hpp"

#include "RpcMessages.pb.h"

#include <util/Epoch.hpp>
#include <util/Logging.hpp>

namespace tell {
namespace store {

namespace {

/**
 * @brief Extracts the snapshot descriptor from the message
 *
 * The data field has to be set in the message.
 */
SnapshotDescriptor snapshotFromMessage(const proto::SnapshotDescriptor& snapshot) {
    auto snapshotLength = snapshot.data().length();
    std::unique_ptr<unsigned char[]> dataBuffer(new unsigned char[snapshotLength]);
    memcpy(dataBuffer.get(), snapshot.data().data(), snapshotLength);
    return SnapshotDescriptor(dataBuffer.release(), snapshotLength, snapshot.version());
}

} // anonymous namespace

ClientConnection::~ClientConnection() {
    boost::system::error_code ec;
    mSocket.close(ec);
    if (ec) {
        // TODO Handle this situation somehow (this should probably not happen at this point)
    }
}

void ClientConnection::init() {
    mSocket.setHandler(this);
}

void ClientConnection::shutdown() {
    boost::system::error_code ec;
    mSocket.disconnect(ec);
    if (ec) {
        LOG_ERROR("Error disconnecting");
        // TODO Handle this situation somehow - Can this even happen?
    }
}

void ClientConnection::onConnected(const boost::system::error_code& ec) {
    if (ec) {
        LOG_ERROR("Failure while establishing client connection [errcode = %1% %2%]", ec, ec.message());
        mManager.removeConnection(this);
    }
}

void ClientConnection::onReceive(const void* buffer, size_t length, const boost::system::error_code& ec) {
    if (ec) {
        LOG_ERROR("Error receiving message");
        // TODO Handle this situation somehow
        return;
    }

    proto::RpcResponseBatch responseBatch;
    proto::RpcRequestBatch requestBatch;
    if (!requestBatch.ParseFromArray(buffer, length)) {
        LOG_ERROR("Error parsing protobuf message");
        // TODO Handle this situation somehow
        return;
    }

    auto maxSize = mSocket.bufferLength();
    auto responseField = responseBatch.mutable_response();
    for (auto& request : requestBatch.request()) {
        auto response = responseField->Add();
        response->set_messageid(request.messageid());
        handleRequest(request, *response);

        // Response batch became too big we have to split it
        if (responseBatch.ByteSize() > maxSize) {
            auto lastResponse = responseField->ReleaseLast();
            LOG_ASSERT(response == lastResponse, "Removed response does not point to added response");

            if (responseField->empty()) {
                // TODO Response too large
            }

            sendResponseBatch(responseBatch);
            responseBatch.Clear();
            responseField->AddAllocated(response);
        }
    }
    // Send remaining response batch
    if (!responseField->empty()) {
        sendResponseBatch(responseBatch);
    }
}

void ClientConnection::onSend(uint32_t userId, const boost::system::error_code& ec) {
    if (ec) {
        LOG_ERROR("Error sending message");
        // TODO Handle this situation somehow
    }
}

void ClientConnection::onDisconnect() {
    shutdown();
}

void ClientConnection::onDisconnected() {
    // Clear snapshot cache - No more handlers are active so we do not need to synchronize
    mSnapshots.clear();

    mManager.removeConnection(this);
}

void ClientConnection::handleRequest(const proto::RpcRequest& request, proto::RpcResponse& response) {
    switch (request.Request_case()) {
    case proto::RpcRequest::kCreateTable: {
        auto& createTableRequest = request.createtable();
        auto createTableResponse = response.mutable_createtable();

        Schema schema(createTableRequest.schema().data());
        uint64_t tableId = 0;
        if (mStorage.createTable(createTableRequest.name(), schema, tableId)) {
            createTableResponse->set_tableid(tableId);
        }
    } break;

    case proto::RpcRequest::kGetTableId: {
        auto& getTableIdRequest = request.gettableid();
        auto getTableIdResponse = response.mutable_gettableid();

        uint64_t tableId = 0;
        if (mStorage.getTableId(getTableIdRequest.name(), tableId)) {
            getTableIdResponse->set_tableid(tableId);
        }
    } break;

    case proto::RpcRequest::kGet: {
        auto& getRequest = request.get();
        handleSnapshot(getRequest.snapshot(), response,
                [this, &getRequest] (const SnapshotDescriptor& snapshot, proto::RpcResponse& response) {
            auto getResponse = response.mutable_get();
            size_t size = 0;
            const char* data = nullptr;
            bool isNewest = false;
            if (mStorage.get(getRequest.tableid(), getRequest.key(), size, data, snapshot, isNewest)) {
                getResponse->set_data(data, size);
            }
            getResponse->set_isnewest(isNewest);
        });
    } break;

    case proto::RpcRequest::kUpdate: {
        auto& updateRequest = request.update();
        handleSnapshot(updateRequest.snapshot(), response,
                [this, &updateRequest] (const SnapshotDescriptor& snapshot, proto::RpcResponse& response) {
            auto updateResponse = response.mutable_update();
            auto& data = updateRequest.data();
            auto succeeded = mStorage.update(updateRequest.tableid(), updateRequest.key(), data.length(), data.data(),
                    snapshot);
            updateResponse->set_succeeded(succeeded);
        });
    } break;

    case proto::RpcRequest::kInsert: {
        auto& insertRequest = request.insert();
        handleSnapshot(insertRequest.snapshot(), response,
                [this, &insertRequest] (const SnapshotDescriptor& snapshot, proto::RpcResponse& response) {
            auto insertResponse = response.mutable_insert();
            bool succeeded = false;
            auto& data = insertRequest.data();
            mStorage.insert(insertRequest.tableid(), insertRequest.key(), data.length(), data.data(), snapshot,
                    (insertRequest.succeeded() ? &succeeded : nullptr));
            if (insertRequest.succeeded()) {
                insertResponse->set_succeeded(succeeded);
            }
        });
    } break;

    case proto::RpcRequest::kRemove: {
        auto& removeRequest = request.remove();
        handleSnapshot(removeRequest.snapshot(), response,
                [this, &removeRequest] (const SnapshotDescriptor& snapshot, proto::RpcResponse& response) {
            auto removeResponse = response.mutable_remove();
            auto succeeded = mStorage.remove(removeRequest.tableid(), removeRequest.key(), snapshot);
            removeResponse->set_succeeded(succeeded);
        });
    } break;

    case proto::RpcRequest::kCommit: {
        auto& commitRequest = request.commit();
        auto commitResponse = response.mutable_commit();

        auto& snapshot = commitRequest.snapshot();
        if (snapshot.cached()) {
            removeSnapshot(snapshot.version());
        }

        // TODO Implement commit logic
        commitResponse->set_succeeded(true);
    } break;

    default: {
        response.set_error(proto::RpcResponse::UNKNOWN_REQUEST);
    } break;
    }
}

void ClientConnection::sendResponseBatch(const proto::RpcResponseBatch& responseBatch) {
    auto sbuffer = mSocket.acquireSendBuffer(responseBatch.ByteSize());
    if (sbuffer.id() == crossbow::infinio::InfinibandBuffer::INVALID_ID) {
        LOG_ERROR("System ran out of buffers");
        // TODO Handle this situation somehow
        return;
    }
    responseBatch.SerializeWithCachedSizesToArray(reinterpret_cast<uint8_t*>(sbuffer.data()));

    boost::system::error_code ec;
    mSocket.send(sbuffer, 0, ec);
    if (ec)  {
        mSocket.releaseSendBuffer(sbuffer);
        LOG_ERROR("Error sending message");
        // TODO Handle this situation somehow
    }
}

template <typename Fun>
void ClientConnection::handleSnapshot(const proto::SnapshotDescriptor& snapshot, proto::RpcResponse& response, Fun f) {
    if (snapshot.cached()) {
        tbb::queuing_rw_mutex::scoped_lock lock(mSnapshotsMutex, false);
        auto i = mSnapshots.find(snapshot.version());

        // Either we already have the snapshot in our cache or the client send it to us
        auto found = (i != mSnapshots.end());
        if (found ^ snapshot.has_data()) {
            response.set_error(proto::RpcResponse::INVALID_SNAPSHOT);
            return;
        }

        if (!found) {
            // We have to add the snapshot to the cache
            auto res = mSnapshots.insert(std::make_pair(snapshot.version(), snapshotFromMessage(snapshot)));
            if (!res.second) { // Element was inserted by another thread
                response.set_error(proto::RpcResponse::INVALID_SNAPSHOT);
                return;
            }
            i = res.first;
        }

        f(i->second, response);
    } else {
        if (!snapshot.has_data()) {
            response.set_error(proto::RpcResponse::INVALID_SNAPSHOT);
            return;
        }
        f(snapshotFromMessage(snapshot), response);
    }
}

void ClientConnection::removeSnapshot(uint64_t version) {
    tbb::queuing_rw_mutex::scoped_lock lock(mSnapshotsMutex, true);
    mSnapshots.unsafe_erase(version);
}

} // namespace store
} // namespace tell
