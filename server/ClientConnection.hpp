#pragma once

#include "ServerConfig.hpp"

#include <tellstore.hpp>

#include <crossbow/infinio/InfinibandBuffer.hpp>
#include <crossbow/infinio/InfinibandSocket.hpp>

#include <tbb/queuing_rw_mutex.h>
#include <tbb/concurrent_unordered_map.h>

#include <boost/system/error_code.hpp>

#include <cstdint>
#include <string>

namespace tell {
namespace store {

namespace proto {
class RpcRequest;
class RpcResponse;
class RpcResponseBatch;
} // namespace proto

class ConnectionManager;

/**
 * @brief The ClientConnection class handles communication with one client
 *
 * Listens for incoming RPC requests, performs the desired action and sends the response back to the client.
 */
class ClientConnection : private crossbow::infinio::InfinibandSocketHandler {
public:
    ClientConnection(ConnectionManager& manager, Storage& storage, crossbow::infinio::InfinibandSocket&& socket)
            : mManager(manager),
              mStorage(storage),
              mSocket(std::move(socket)) {
    }

    ~ClientConnection();

    void init();

    void shutdown();

private:
    virtual void onConnected(const boost::system::error_code& ec) final override;

    virtual void onReceive(const void* buffer, size_t length, const boost::system::error_code& ec) final override;

    virtual void onSend(uint32_t userId, const boost::system::error_code& ec) final override;

    virtual void onDisconnect() final override;

    virtual void onDisconnected() final override;

    void closeConnection();

    void handleRequest(const proto::RpcRequest& request, proto::RpcResponse& response);

    void sendResponseBatch(const proto::RpcResponseBatch& responseBatch);

    /**
     * @brief Get the transaction associated with the request
     *
     * Retrieves the transaction from the cache if it is present. If the client sent a snapshot descriptor with the
     * request a new transaction is created and added to the cache. The transaction has to be either in the cache or
     * sent with the request (not both and not none of them at the same time).
     *
     * @param request Client request to get the associated transaction
     * @return A pointer to the transaction or null in case the transaction is invalid
     */
    Storage::Transaction* getTransaction(const proto::RpcRequest& request);

    /**
     * @brief Removes the transaction from the cache
     */
    void removeTransaction(uint64_t version);

    ConnectionManager& mManager;
    Storage& mStorage;

    /// Client socket
    crossbow::infinio::InfinibandSocket mSocket;

    /// Mutex for the transaction cache
    tbb::queuing_rw_mutex mTransactionsMutex;

    /// Transaction Cache mapping the version number to the transaction
    tbb::concurrent_unordered_map<uint64_t, Storage::Transaction> mTransactions;

};

} // namespace store
} // namespace tell
