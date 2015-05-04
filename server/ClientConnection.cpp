#include "ClientConnection.hpp"

#include "ConnectionManager.hpp"

#include "RpcMessages.pb.h"

#include <util/Epoch.hpp>
#include <util/Logging.hpp>
#include <util/SnapshotDescriptor.hpp>

namespace tell {
namespace store {

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

void ClientConnection::onReceive(const crossbow::infinio::InfinibandBuffer& buffer, size_t length,
        const boost::system::error_code& ec) {
    if (ec) {
        LOG_ERROR("Error receiving message");
        // TODO Handle this situation somehow
        return;
    }

    proto::RpcResponse response;
    proto::RpcRequest request;
    if (!request.ParseFromArray(buffer.data(), length)) {
        LOG_ERROR("Error parsing protobuf message");
        // TODO Handle this situation somehow
        return;
    }
    handleRequest(request, response);

    auto sbuffer = mSocket.acquireBuffer(response.ByteSize());
    if (sbuffer.id() == crossbow::infinio::InfinibandBuffer::INVALID_ID) {
        LOG_ERROR("System ran out of buffers");
        // TODO Handle this situation somehow
        return;
    }
    response.SerializeWithCachedSizesToArray(reinterpret_cast<uint8_t*>(sbuffer.data()));

    boost::system::error_code ec2;
    mSocket.send(sbuffer, ec2);
    if (ec2)  {
        mSocket.releaseBuffer(sbuffer.id());
        LOG_ERROR("Error sending message");
        // TODO Handle this situation somehow
    }
}

void ClientConnection::onSend(const crossbow::infinio::InfinibandBuffer& buffer, size_t length,
        const boost::system::error_code& ec) {
    if (ec) {
        LOG_ERROR("Error sending message");
        // TODO Handle this situation somehow
    }
}

void ClientConnection::onDisconnect() {
    shutdown();
}

void ClientConnection::onDisconnected() {
    // Abort all open transactions
    // No more handlers are active so we do not need to synchronize
    for (auto& transaction : mTransactions) {
        transaction.second.abort();
    }

    mManager.removeConnection(this);
}

void ClientConnection::handleRequest(const proto::RpcRequest& request, proto::RpcResponse& response) {
    response.set_version(request.version());

    auto transaction = getTransaction(request);
    if (!transaction) {
        response.set_error(proto::RpcResponse::INVALID_DESCRIPTOR);
        return;
    }

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
        auto getResponse = response.mutable_get();

        size_t size = 0;
        const char* data = nullptr;
        bool isNewest = false;
        if (mStorage.get(getRequest.tableid(), getRequest.key(), size, data, transaction->descriptor(), isNewest)) {
            getResponse->set_data(data, size);
        }
        getResponse->set_isnewest(isNewest);
    } break;

    case proto::RpcRequest::kUpdate: {
        auto& updateRequest = request.update();
        auto updateResponse = response.mutable_update();

        auto& data = updateRequest.data();
        auto succeeded = mStorage.update(updateRequest.tableid(), updateRequest.key(), data.length(), data.data(),
                transaction->descriptor());
        updateResponse->set_succeeded(succeeded);
    } break;

    case proto::RpcRequest::kInsert: {
        auto& insertRequest = request.insert();
        auto insertResponse = response.mutable_insert();

        bool succeeded = false;
        auto& data = insertRequest.data();
        mStorage.insert(insertRequest.tableid(), insertRequest.key(), data.length(), data.data(),
                transaction->descriptor(), (insertRequest.succeeded() ? &succeeded : nullptr));
        if (insertRequest.succeeded()) {
            insertResponse->set_succeeded(succeeded);
        }
    } break;

    case proto::RpcRequest::kRemove: {
        auto& removeRequest = request.remove();
        auto removeResponse = response.mutable_remove();

        auto succeeded = mStorage.remove(removeRequest.tableid(), removeRequest.key(), transaction->descriptor());
        removeResponse->set_succeeded(succeeded);
    } break;

    case proto::RpcRequest::kCommit: {
        auto& commitRequest = request.commit();
        auto commitResponse = response.mutable_commit();

        if (commitRequest.commit()) {
            transaction->commit();
        } else {
            transaction->abort();
        }
        commitResponse->set_succeeded(true);
    } break;

    default: {
        response.set_error(proto::RpcResponse::UNKNOWN_REQUEST);
    } break;
    }
}

Storage::Transaction* ClientConnection::getTransaction(const proto::RpcRequest& request) {
    Storage::Transaction* transaction;
    {
        tbb::queuing_rw_mutex::scoped_lock lock(mTransactionsMutex, false);
        auto i = mTransactions.find(request.version());
        transaction = (i != mTransactions.end() ? &i->second : nullptr);
    }

    // Check if the transaction was either in the cache or a descriptor was sent (not both and not none of them)
    if (!!transaction ^ request.has_desc()) {
        return nullptr;
    }

    // We have the transaction cached
    if (transaction) {
        return transaction;
    }

    // We have to add the transaction to the cache
    auto descriptorLength = request.desc().length();
    std::unique_ptr<unsigned char[]> dataBuffer(new unsigned char[descriptorLength]);
    memcpy(dataBuffer.get(), request.desc().data(), descriptorLength);
    SnapshotDescriptor descriptor(dataBuffer.release(), descriptorLength, request.version());

    tbb::queuing_rw_mutex::scoped_lock lock(mTransactionsMutex, false);
    auto res = mTransactions.insert(std::make_pair(request.version(), Storage::Transaction(mStorage,
            std::move(descriptor))));
    if (!res.second) {
        return nullptr;
    }
    return &res.first->second;
}

void ClientConnection::removeTransaction(uint64_t version) {
    tbb::queuing_rw_mutex::scoped_lock lock(mTransactionsMutex, true);
    mTransactions.unsafe_erase(version);
}

} // namespace store
} // namespace tell
